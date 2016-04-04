package main

import (
	"fmt"
	"github.com/pagarme/teleport/config"
	"github.com/pagarme/teleport/server"
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

	// Install triggers for each target
	for _, target := range config.Targets {
		config.Database.InstallTriggers(target.SourceTables)
	}

	// Watch events
	go config.Database.BatchEvents(5 * time.Second)

	// go config.Database.Re(5 * time.Second)

	server := server.New(&config.Database, config.ServerHTTP)

	// Start HTTP server
	if err = server.Start(); err != nil {
		fmt.Printf("ERROR STARTING SERVER: %v\n", err)
	}
}
