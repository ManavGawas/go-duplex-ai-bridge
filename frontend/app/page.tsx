"use client";

import { useRef, useState } from "react";

export default function Home() {
  const [isRecording, setIsRecording] = useState(false);
  const socketRef = useRef<WebSocket | null>(null);
  const mediaRecorderRef = useRef<MediaRecorder | null>(null);
  const audioContextRef = useRef<AudioContext | null>(null);
  
  // ARCHITECT FIX: React hooks must be inside the component
  const nextAudioTimeRef = useRef<number>(0);

  const startRecording = async () => {
    try {
      // THE FIX: Reset the timeline for a new conversation
      nextAudioTimeRef.current = 0;

      // 1. Initialize Audio Context for playing AI responses
      // We initialize this on user interaction (button click) to comply with browser autoplay policies
      const AudioContext = window.AudioContext || (window as any).webkitAudioContext;
      audioContextRef.current = new AudioContext();

      // 2. Establish connection to the Go Routing Core
      socketRef.current = new WebSocket("ws://localhost:8080/ws");
      
      socketRef.current.onopen = () => {
        console.log("Connected to Go Routing Core.");
      };

      // 3. Handle Incoming Data (Audio OR JSON Control Messages)
      socketRef.current.onmessage = async (event) => {
        
        // VAD FIX: If the backend sends a text message, it's a kill signal
        if (typeof event.data === "string") {
          const msg = JSON.parse(event.data);
          if (msg.action === "interrupt") {
            console.log("Interruption detected! Flushing audio queue.");
            // Instantly kill the current audio context to stop playback
            if (audioContextRef.current) {
              await audioContextRef.current.close();
            }
            // Spin up a fresh one for the next response
            const AudioContext = window.AudioContext || (window as any).webkitAudioContext;
            audioContextRef.current = new AudioContext();
            nextAudioTimeRef.current = 0; 
          }
          return;
        }

        // QUEUED PLAYBACK (Existing Logic for Blobs)
        if (event.data instanceof Blob) {
          try {
            const arrayBuffer = await event.data.arrayBuffer();
            const audioBuffer = await audioContextRef.current!.decodeAudioData(arrayBuffer);
            const source = audioContextRef.current!.createBufferSource();
            source.buffer = audioBuffer;
            source.connect(audioContextRef.current!.destination);

            const currentTime = audioContextRef.current!.currentTime;
            if (nextAudioTimeRef.current < currentTime) {
              nextAudioTimeRef.current = currentTime;
            }

            source.start(nextAudioTimeRef.current);
            nextAudioTimeRef.current += audioBuffer.duration;
          } catch (err) {
            console.error("Failed to decode audio", err);
          }
        }
      };

      // 4. Request Microphone Access
      const stream = await navigator.mediaDevices.getUserMedia({ 
        audio: {
            channelCount: 1, // Mono audio required by Deepgram
            sampleRate: 16000 // Standard AI telephony sample rate
        } 
      });
      
      // 5. Initialize MediaRecorder
      const mediaRecorder = new MediaRecorder(stream);
      mediaRecorderRef.current = mediaRecorder;

      // 6. Capture and Send Audio Chunks
      mediaRecorder.ondataavailable = async (event) => {
        if (event.data.size > 0 && socketRef.current?.readyState === WebSocket.OPEN) {
          const buffer = await event.data.arrayBuffer();
          socketRef.current.send(buffer);
        }
      };

      // 7. Start recording and emit a chunk every 250 milliseconds
      mediaRecorder.start(250);
      setIsRecording(true);

    } catch (error) {
      console.error("Error accessing microphone or websocket:", error);
    }
  };

  const stopRecording = () => {
    if (mediaRecorderRef.current) {
      mediaRecorderRef.current.stop();
      mediaRecorderRef.current.stream.getTracks().forEach(track => track.stop());
    }
    if (socketRef.current) {
      socketRef.current.close();
    }
    if (audioContextRef.current) {
      audioContextRef.current.close();
    }
    setIsRecording(false);
  };

  return (
    <main className="flex min-h-screen flex-col items-center justify-center bg-gray-950 text-white p-24">
      <h1 className="text-4xl font-bold mb-8 tracking-tight">Syncora Voice Core</h1>
      
      <div className="flex gap-4">
        {!isRecording ? (
          <button 
            onClick={startRecording}
            className="px-6 py-3 bg-white text-black font-semibold rounded-lg hover:bg-gray-200 transition-colors shadow-lg"
          >
            Start Streaming
          </button>
        ) : (
          <button 
            onClick={stopRecording}
            className="px-6 py-3 bg-red-500 text-white font-semibold rounded-lg hover:bg-red-600 transition-colors shadow-lg shadow-red-500/20"
          >
            Stop Streaming
          </button>
        )}
      </div>
      
      {isRecording && (
        <div className="mt-8 flex items-center gap-2">
          <div className="w-3 h-3 bg-red-500 rounded-full animate-pulse"></div>
          <p className="text-sm text-gray-400 font-mono">Pipeline Active: Listening & Streaming...</p>
        </div>
      )}
    </main>
  );
}