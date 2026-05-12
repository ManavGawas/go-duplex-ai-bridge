package main

import (
	"log"
	"net/http"

	"github.com/ManavGawas/syncora-voice-core/internal/transport"
	"github.com/joho/godotenv"
)

func main() {
	// Attempt to load .env from the current directory
	err := godotenv.Load()
	if err != nil {
		log.Println("Note: .env not found in current directory. Ensure keys are in your environment variables.")
	}

	http.HandleFunc("/ws", transport.HandleClientConnection)

	log.Println("Syncora Voice Core routing on :8080")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}
