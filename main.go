package main

import (
	"fmt"
	"github.com/pagarme/teleport/config"
)

func main() {
	config := config.New()

	config.ReadFromFile("config.yml")

	fmt.Printf("config: %v\n", config)
}
