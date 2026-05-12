package transport

import (
	"context"
	"encoding/json"
	"log"
	"net/http"

	"github.com/ManavGawas/syncora-voice-core/internal/ai"
	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true // Allow all origins for local development
	},
}

func HandleClientConnection(w http.ResponseWriter, r *http.Request) {
	// 1. Accept browser connection
	clientConn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("Failed to upgrade client connection: %v", err)
		return
	}
	defer clientConn.Close()
	log.Println("Client connected. Initializing AI Pipeline...")

	// 2. Open connection to Deepgram
	dgConn, err := ai.ConnectDeepgram()
	if err != nil {
		log.Printf("Error: %v", err)
		return
	}
	defer dgConn.Close()

	// VAD ARCHITECTURE: State tracking for the active AI generation
	var aiCtx context.Context
	var cancelAI context.CancelFunc
	// Initialize a dummy context to start
	aiCtx, cancelAI = context.WithCancel(context.Background())

	// 3. GOROUTINE: Listen for transcripts from Deepgram asynchronously
	go func() {
		for {
			_, message, err := dgConn.ReadMessage()
			if err != nil {
				log.Println("Deepgram stream closed.")
				return
			}

			var dgResp ai.DeepgramResponse
			if err := json.Unmarshal(message, &dgResp); err == nil {

				// Ensure there is actually a transcript returned
				if len(dgResp.Channel.Alternatives) > 0 {
					transcript := dgResp.Channel.Alternatives[0].Transcript

					if transcript != "" {
						if dgResp.IsFinal {
							// Green text for finalized sentences
							log.Printf("\033[32m[User]\033[0m %s", transcript)

							// --- START AI PIPELINE ---
							// 1. Cancel any lingering generation, start a fresh turn
							cancelAI()
							aiCtx, cancelAI = context.WithCancel(context.Background())

							tokenChan := make(chan string)
							audioChan := make(chan []byte, 100)

							// 2. Pass the new cancelable context to Groq
							go ai.StreamLLMResponse(aiCtx, transcript, tokenChan)

							// 3. Audio Sender Goroutine (Listens for Cancellation)
							go func(ctx context.Context) {
								for {
									select {
									case <-ctx.Done():
										return // Die instantly if interrupted
									case audioChunk := <-audioChan:
										err := clientConn.WriteMessage(websocket.BinaryMessage, audioChunk)
										if err != nil {
											log.Printf("Failed to send audio to client: %v", err)
											return
										}
									}
								}
							}(aiCtx)

							// 4. Semantic Buffer Goroutine (Listens for Cancellation)
							go func(ctx context.Context) {
								var sentenceBuffer string
								for {
									select {
									case <-ctx.Done():
										return // Die instantly if interrupted
									case token, ok := <-tokenChan:
										if !ok {
											// Groq channel closed (finished thinking). Flush any remaining text.
											if sentenceBuffer != "" {
												go ai.StreamTTS(ctx, sentenceBuffer, audioChan)
											}
											return
										}

										sentenceBuffer += token

										// Flush to TTS on punctuation
										if len(token) > 0 && (token[len(token)-1] == '.' || token[len(token)-1] == '!' || token[len(token)-1] == '?' || token[len(token)-1] == ',') {
											textToSpeak := sentenceBuffer
											sentenceBuffer = ""
											// Pass context to Cartesia
											go ai.StreamTTS(ctx, textToSpeak, audioChan)
										}
									}
								}
							}(aiCtx)

						} else {
							// --- VAD: THE INTERRUPTION TRIGGER ---
							// User is speaking! (Gray text for interim words)
							log.Printf("\033[90m[Interim]\033[0m %s", transcript)

							// 1. Instantly kill Groq and Cartesia HTTP requests mid-flight
							cancelAI()

							// 2. Tell Next.js to flush its audio queue immediately
							err := clientConn.WriteMessage(websocket.TextMessage, []byte(`{"action":"interrupt"}`))
							if err != nil {
								log.Printf("Failed to send interrupt signal: %v", err)
							}
						}
					}
				}
			}
		}
	}()

	// 4. MAIN THREAD: Route browser audio bytes directly to Deepgram
	for {
		messageType, message, err := clientConn.ReadMessage()
		if err != nil {
			log.Println("Client disconnected.")
			// Trigger cancel to clean up any running Goroutines when the user leaves
			cancelAI()
			break
		}

		if messageType == websocket.BinaryMessage {
			// Pipe the raw bytes straight to Deepgram
			err = dgConn.WriteMessage(websocket.BinaryMessage, message)
			if err != nil {
				log.Printf("Failed to pipe audio to Deepgram: %v", err)
				break
			}
		}
	}
}
