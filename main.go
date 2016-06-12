package main

import (
	"flag"
	"log"
	"os"
	"time"

	"github.com/pagarme/teleport/applier"
	"github.com/pagarme/teleport/batcher"
	"github.com/pagarme/teleport/client"
	"github.com/pagarme/teleport/config"
	"github.com/pagarme/teleport/database"
	"github.com/pagarme/teleport/ddlwatcher"
	"github.com/pagarme/teleport/loader"
	"github.com/pagarme/teleport/server"
	"github.com/pagarme/teleport/transmitter"
	"github.com/pagarme/teleport/vacuum"
)

func main() {
	// Parse config
	configPath := flag.String("config", "config.yml", "config file path")
	mode := flag.String("mode", "replication", "teleport mode [replication|initial-load]")
	loadTarget := flag.String("load-target", "", "target to perform initial load [target name]")
	flag.Parse()

	// Load config file
	config := config.New()
	err := config.ReadFromFile(*configPath)

	if err != nil {
		log.Printf("Error opening config file '%s': %v\n", *configPath, err)
		os.Exit(1)
	}

	invalidProcessingInterval :=
		config.ProcessingIntervals.Batcher == 0 ||
			config.ProcessingIntervals.Transmitter == 0 ||
			config.ProcessingIntervals.Applier == 0 ||
			config.ProcessingIntervals.Vacuum == 0 ||
			config.ProcessingIntervals.DdlWatcher == 0

	if invalidProcessingInterval {
		log.Printf("Invalid config value 0 for ProcessingInterval\n")
		os.Exit(1)
	}

	if config.BatchSize == 0 {
		log.Printf("Invalid config value 0 for BatchSize\n")
		os.Exit(1)
	}

	db := database.New(config.Database)

	// Start db
	if err = db.Start(); err != nil {
		log.Printf("Erro starting database: ", err)
		os.Exit(1)
	}

	targets := make(map[string]*client.Client)

	// Create a new client for each target
	// Install triggers for each target
	for key, target := range config.Targets {
		targets[key] = client.New(target)
		db.InstallTriggers(targets[key].TargetExpression)
	}

	if *mode == "replication" {
		// Start batcher on a separate goroutine
		batcher := batcher.New(db, targets)
		go batcher.Watch(time.Duration(config.ProcessingIntervals.Batcher) * time.Millisecond)

		// Start transmitter on a separate goroutine
		transmitter := transmitter.New(db, targets)
		go transmitter.Watch(time.Duration(config.ProcessingIntervals.Transmitter) * time.Millisecond)

		// Start applier on a separate goroutine
		applier := applier.New(db, config.BatchSize)
		go applier.Watch(time.Duration(config.ProcessingIntervals.Applier) * time.Millisecond)

		// Start vacuum on a separate goroutine
		vacuum := vacuum.New(db)
		go vacuum.Watch(time.Duration(config.ProcessingIntervals.Vacuum) * time.Millisecond)

		if len(targets) > 0 {
			// Start vacuum on a separate goroutine if there's
			// any target that needs to know about DDL changes
			ddlwatcher := ddlwatcher.New(db)
			go ddlwatcher.Watch(time.Duration(config.ProcessingIntervals.DdlWatcher) * time.Millisecond)
		}

		// Start HTTP server for receiving incoming requests
		server := server.New(db, config.ServerHTTP)

		// Start HTTP server
		if err = server.Start(); err != nil {
			log.Printf("Error starting HTTP server: %v\n", err)
			os.Exit(1)
		}
	} else if *mode == "initial-load" {
		target, ok := targets[*loadTarget]

		if !ok {
			log.Printf("Error starting loader: target %s not found!\n", *loadTarget)
			os.Exit(1)
		}

		loader := loader.New(db, target, *loadTarget, config.BatchSize)
		err := loader.Load()

		if err != nil {
			log.Printf("Error creating events: %#v\n", err)
		}
	}
}
