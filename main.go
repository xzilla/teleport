package main

import (
	"fmt"
	"github.com/pagarme/teleport/config"
)

func main() {
	config := config.New()

	// Load config file
	err := config.ReadFromFile("source_config.yml")

	// Start db
	if err = config.Database.Start(); err != nil {
		fmt.Printf("ERROR STARTING DATABASE: %v\n", err)
	}

	for _, target := range config.Targets {
		config.Database.InstallTriggers(target.SourceTables)
	}

	config.Database.WatchEvents(5)
}
