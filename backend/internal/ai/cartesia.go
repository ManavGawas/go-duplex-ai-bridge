package ai

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"os"
)

const cartesiaURL = "https://api.cartesia.ai/tts/bytes"

// StreamTTS takes a text chunk, calls Cartesia, and pipes the WAV audio into the channel
func StreamTTS(ctx context.Context, text string, audioChan chan<- []byte) {
	apiKey := os.Getenv("CARTESIA_API_KEY")
	if apiKey == "" {
		log.Println("Error: CARTESIA_API_KEY is missing from .env")
		return
	}

	reqBody, _ := json.Marshal(map[string]interface{}{
		"model_id":   "sonic-english",
		"transcript": text,
		"voice": map[string]interface{}{
			"mode": "id",
			"id":   "a0e99841-438c-4a64-b679-ae501e7d6091", // Standard Cartesia Voice ID
		},
		"output_format": map[string]interface{}{
			"container":   "wav",
			"encoding":    "pcm_f32le",
			"sample_rate": 44100,
		},
	})

	req, _ := http.NewRequest("POST", cartesiaURL, bytes.NewBuffer(reqBody))
	req.Header.Set("X-API-Key", apiKey)
	req.Header.Set("Cartesia-Version", "2024-06-10")
	req.Header.Set("Content-Type", "application/json")

	// VAD ARCHITECTURE: Attach the context to kill the HTTP request mid-flight
	req = req.WithContext(ctx)

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		// Ignore error if we intentionally killed the connection
		if ctx.Err() == context.Canceled {
			return
		}
		log.Printf("Cartesia network request failed: %v", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		log.Printf("Cartesia API Error (HTTP %d): %s", resp.StatusCode, string(bodyBytes))
		return
	}

	// Final check before reading the body in case we were canceled during the network round-trip
	if ctx.Err() != nil {
		return
	}

	// Read the complete WAV file for this specific sentence chunk and send it to the frontend
	bodyBytes, err := io.ReadAll(resp.Body)
	if err == nil && len(bodyBytes) > 0 {
		// Safely send to channel, aborting if the context was canceled while reading
		select {
		case <-ctx.Done():
			return
		default:
			audioChan <- bodyBytes
		}
	}
}
