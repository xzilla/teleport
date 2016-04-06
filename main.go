package main

import (
	"fmt"
	"github.com/pagarme/teleport/config"
	"github.com/pagarme/teleport/client"
	"github.com/pagarme/teleport/server"
	"github.com/pagarme/teleport/batcher"
	"github.com/pagarme/teleport/transmitter"
	"time"
	"flag"
	"os"
)

func main() {
	// Parse config
	configPath := flag.String("config", "config.yml", "config file path")
	flag.Parse()

	// Load config file
	config := config.New()
	err := config.ReadFromFile(*configPath)

	if err != nil {
		fmt.Printf("Error opening config file '%s': %v\n", *configPath, err)
		os.Exit(1)
	}

	// Start db
	if err = config.Database.Start(); err != nil {
		fmt.Printf("Erro starting database: ", err)
		os.Exit(1)
	}

	targets := make(map[string]*client.Client)

	// Create a new client for each target
	// Install triggers for each target
	for key, target := range config.Targets {
		targets[key] = client.New(target)
		config.Database.InstallTriggers(targets[key].SourceTables)
	}

	// Start batcher on a separate goroutine
	batcher := batcher.New(&config.Database, targets)
	go batcher.Watch(5 * time.Second)

	// Start transmitter on a separate goroutine
	transmitter := transmitter.New(&config.Database, targets)
	go transmitter.Watch(5 * time.Second)

	// Start HTTP server for receiving incoming requests
	server := server.New(&config.Database, config.ServerHTTP)

	// Start HTTP server
	if err = server.Start(); err != nil {
		fmt.Printf("Error starting HTTP server: %v\n", err)
		os.Exit(1)
	}
}
