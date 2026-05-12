package ai

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"os"

	"github.com/gorilla/websocket"
)

type DeepgramResponse struct {
	IsFinal bool `json:"is_final"`
	Channel struct {
		Alternatives []struct {
			Transcript string `json:"transcript"`
		} `json:"alternatives"`
	} `json:"channel"`
}

func ConnectDeepgram() (*websocket.Conn, error) {
	apiKey := os.Getenv("DEEPGRAM_API_KEY")
	if apiKey == "" {
		return nil, fmt.Errorf("DEEPGRAM_API_KEY is missing from .env")
	}

	// We removed strict encoding limits to let Deepgram auto-detect the browser's WebM stream
	// We are using nova-2 to ensure strict free-tier compatibility
	url := "wss://api.deepgram.com/v1/listen?model=nova-2&interim_results=true&smart_format=true"

	headers := http.Header{}
	headers.Add("Authorization", "Token "+apiKey)

	dialer := websocket.DefaultDialer
	conn, resp, err := dialer.Dial(url, headers)
	if err != nil {
		// ARCHITECT'S DEBUG: If the handshake fails, read the exact response body
		if resp != nil {
			bodyBytes, _ := io.ReadAll(resp.Body)
			return nil, fmt.Errorf("bad handshake. HTTP %d. Deepgram says: %s", resp.StatusCode, string(bodyBytes))
		}
		return nil, fmt.Errorf("Deepgram connection failed entirely: %v", err)
	}

	log.Println("Deepgram STT Engine connected successfully.")
	return conn, nil
}
