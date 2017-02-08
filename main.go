package main

import (
	"flag"
	log "github.com/Sirupsen/logrus"
	"github.com/evalphobia/logrus_sentry"
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
	// Startup message. It's useful to get the logs of the last successful run.
	// In order to use it, run `docker-compose logs (source|target) | sed -n '/Teleport Started/{h;b};H;${x;p}'`
	log.Info("[Teleport Started]")

	// Parse config
	configPath := flag.String("config", "config.yml", "config file path")
	mode := flag.String("mode", "replication", "teleport mode [replication|initial-load]")
	loadTarget := flag.String("load-target", "", "target to perform initial load [target name]")
	flag.Parse()

	// Load config file
	config := config.New()
	err := config.ReadFromFile(*configPath)

	if err != nil {
		log.Panicf("Error opening config file '%s': %v", *configPath, err)
	}

	if config.SentryEndpoint != "" {
		hook, err := logrus_sentry.NewSentryHook(config.SentryEndpoint, []log.Level{
			log.PanicLevel,
			log.FatalLevel,
			log.ErrorLevel,
			log.InfoLevel,
		})

		if err != nil {
			log.Panicf("Error initializing sentry: %v", err)
		}

		log.AddHook(hook)
	}

	invalidProcessingInterval :=
		config.ProcessingIntervals.Batcher == 0 ||
			config.ProcessingIntervals.Transmitter == 0 ||
			config.ProcessingIntervals.Applier == 0 ||
			config.ProcessingIntervals.Vacuum == 0 ||
			config.ProcessingIntervals.DdlWatcher == 0

	if invalidProcessingInterval {
		log.Panicf("Invalid config value 0 for ProcessingInterval")
	}

	if config.BatchSize == 0 {
		log.Panicf("Invalid config value 0 for BatchSize")
	}

	db := database.New(config.Database)

	// Start db
	if err = db.Start(); err != nil {
		log.Panicf("Error starting database: %v", err)
	}

	targets := make(map[string]*client.Client)

	// Create a new client for each target
	// Install triggers for each target
	for key, target := range config.Targets {
		targets[key] = client.New(target)
		db.InstallTriggers(targets[key].TargetExpression)
	}

	if *mode == "replication" {
		// Only start the following coroutines if there are targets to send data to
		if len(targets) > 0 {
			// Start batcher on a separate goroutine
			batcher := batcher.New(db, targets, config.MaxEventsPerBatch)
			go batcher.Watch(time.Duration(config.ProcessingIntervals.Batcher) * time.Millisecond)

			// Start transmitter on a separate goroutine
			transmitter := transmitter.New(db, targets)
			go transmitter.Watch(time.Duration(config.ProcessingIntervals.Transmitter) * time.Millisecond)

			// Start DDL watcher on a separate goroutine
			ddlwatcher := ddlwatcher.New(db)
			go ddlwatcher.Watch(time.Duration(config.ProcessingIntervals.DdlWatcher) * time.Millisecond)
		}

		// Start applier on a separate goroutine
		applier := applier.New(db, config.BatchSize)
		go applier.Watch(time.Duration(config.ProcessingIntervals.Applier) * time.Millisecond)

		// Start vacuum on a separate goroutine
		vacuum := vacuum.New(db)
		go vacuum.Watch(time.Duration(config.ProcessingIntervals.Vacuum) * time.Millisecond)

		// Start HTTP server for receiving incoming requests
		server := server.New(db, config.ServerHTTP)

		// Start HTTP server
		if err = server.Start(); err != nil {
			log.Panicf("Error starting HTTP server: %v", err)
		}
	} else if *mode == "initial-load" {
		target, ok := targets[*loadTarget]

		if !ok {
			log.Panicf("Error starting loader: target %s not found!", *loadTarget)
		}

		loader := loader.New(db, target, *loadTarget, config.BatchSize, config.MaxEventsPerBatch)
		err := loader.Load()

		if err != nil {
			log.Errorf("Error creating events: %#v", err)
		}
	}
}
