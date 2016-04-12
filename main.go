package main

import (
	"flag"
	"fmt"
	"github.com/pagarme/teleport/applier"
	"github.com/pagarme/teleport/batcher"
	"github.com/pagarme/teleport/client"
	"github.com/pagarme/teleport/config"
	"github.com/pagarme/teleport/database"
	"github.com/pagarme/teleport/server"
	"github.com/pagarme/teleport/transmitter"
	"os"
	"time"
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

	db := database.New(
		config.Database.Name,
		config.Database.Database,
		config.Database.Hostname,
		config.Database.Username,
		config.Database.Password,
		config.Database.Port,
	)

	// Start db
	if err = db.Start(); err != nil {
		fmt.Printf("Erro starting database: ", err)
		os.Exit(1)
	}

	targets := make(map[string]*client.Client)

	// Create a new client for each target
	// Install triggers for each target
	for key, target := range config.Targets {
		targets[key] = client.New(target)
		db.InstallTriggers(targets[key].TargetExpression)
	}

	// Start batcher on a separate goroutine
	batcher := batcher.New(db, targets)
	go batcher.Watch(5 * time.Second)

	// Start transmitter on a separate goroutine
	transmitter := transmitter.New(db, targets)
	go transmitter.Watch(5 * time.Second)

	// Start applier on a separate goroutine
	applier := applier.New(db)
	go applier.Watch(5 * time.Second)

	// Start HTTP server for receiving incoming requests
	server := server.New(db, config.ServerHTTP)

	// Start HTTP server
	if err = server.Start(); err != nil {
		fmt.Printf("Error starting HTTP server: %v\n", err)
		os.Exit(1)
	}
}
