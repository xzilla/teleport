package main

import (
	"fmt"
	"github.com/pagarme/teleport/config"
)

func main() {
	config := config.New()

	config.ReadFromFile("config.yml")

	source := config.Databases["source"]

	if err := source.Start(); err != nil {
		fmt.Printf("ERROR STARTING DATABASE: %v\n", err)
	}

	fmt.Printf("source: %v\n", source)
}
