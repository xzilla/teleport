package main

import (
	"fmt"
	"github.com/pagarme/teleport/config"
)

func main() {
	config := config.New()

	config.ReadFromFile("config.yml")

	source := config.Databases["source"]

	if err := source.Connect(); err != nil {
		fmt.Printf("ERROR CONNECTING TO DATABASE: %v\n", err)
	}

	fmt.Printf("source: %v\n", source)
}
