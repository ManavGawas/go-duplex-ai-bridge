package ai

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
)

const groqURL = "https://api.groq.com/openai/v1/chat/completions"

// StreamLLMResponse takes a prompt, calls Groq, and pipes tokens into a Go channel
func StreamLLMResponse(ctx context.Context, prompt string, tokenChan chan<- string) {
	defer close(tokenChan) // Ensure channel always closes when this function exits

	apiKey := os.Getenv("GROQ_API_KEY")
	if apiKey == "" {
		log.Println("Groq Error: GROQ_API_KEY is missing from .env")
		return
	}

	// 1. Construct the payload
	reqBody, _ := json.Marshal(map[string]interface{}{
		"model": "llama-3.1-8b-instant",
		"messages": []map[string]string{
			{
				"role":    "system",
				"content": "You are a fast, concise voice assistant. Respond in 1-2 short sentences. Do not use formatting.",
			},
			{
				"role":    "user",
				"content": prompt,
			},
		},
		"stream": true,
	})

	req, _ := http.NewRequest("POST", groqURL, bytes.NewBuffer(reqBody))
	req.Header.Set("Authorization", "Bearer "+apiKey)
	req.Header.Set("Content-Type", "application/json")

	// VAD ARCHITECTURE: Attach the context to kill the HTTP request mid-flight
	req = req.WithContext(ctx)

	// 2. Execute the request
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		// Ignore error if we intentionally killed the connection
		if ctx.Err() == context.Canceled {
			log.Println("Groq stream gracefully aborted by user interruption.")
			return
		}
		log.Printf("Groq network request failed: %v", err)
		return
	}
	defer resp.Body.Close()

	// 3. ARCHITECT'S DEBUG: Check for non-200 HTTP status
	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		log.Printf("Groq API Error (HTTP %d): %s", resp.StatusCode, string(bodyBytes))
		return
	}

	// 4. Parse the Server-Sent Events (SSE) stream safely
	scanner := bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		// Instantly exit the loop if the kill signal is received
		select {
		case <-ctx.Done():
			return
		default:
		}

		line := scanner.Text()

		if strings.HasPrefix(line, "data: ") {
			data := strings.TrimPrefix(line, "data: ")

			if data == "[DONE]" {
				break
			}

			var chunk map[string]interface{}
			if err := json.Unmarshal([]byte(data), &chunk); err != nil {
				continue // Skip malformed chunks
			}

			// Safely traverse the JSON map without panicking
			if choices, ok := chunk["choices"].([]interface{}); ok && len(choices) > 0 {
				if choice, ok := choices[0].(map[string]interface{}); ok {
					if delta, ok := choice["delta"].(map[string]interface{}); ok {
						if content, ok := delta["content"].(string); ok {
							tokenChan <- content
						}
					}
				}
			}
		}
	}

	if err := scanner.Err(); err != nil {
		if ctx.Err() != context.Canceled {
			log.Printf("Error reading Groq stream: %v", err)
		}
	}
}
