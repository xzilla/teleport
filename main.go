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

	rows, err := source.RunQuery("SELECT 5+5;")

	fmt.Printf("err: %v\nrows: %v\n", err, rows)

	fmt.Printf("source: %v\n", source)
}
