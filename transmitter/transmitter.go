package transmitter

import (
	"github.com/pagarme/teleport/client"
	"github.com/pagarme/teleport/database"
	"log"
	"time"
)

type Transmitter struct {
	db      *database.Database
	clients map[string]*client.Client
}

func New(db *database.Database, clients map[string]*client.Client) *Transmitter {
	return &Transmitter{
		db:      db,
		clients: clients,
	}
}

// Transmit batches
func (t *Transmitter) Watch(sleepTime time.Duration) {
	for {
		batches, err := t.db.GetBatches("waiting_transmission")

		if err != nil {
			log.Printf("Error fetching batches for transmission! %v\n", err)
		} else {
			for _, batch := range batches {
				err := t.Transmit(&batch)

				if err != nil {
					log.Printf("Error transmitting batch! %v\n", err)
				}
			}
		}

		time.Sleep(sleepTime)
	}
}

// Transmit a batch to a target
func (t *Transmitter) Transmit(batch *database.Batch) error {
	client := t.clients[batch.Target]

	_, err := client.SendRequest("/batches", batch)

	if err != nil {
		return err
	}

	return t.markBatchTransmitted(batch)
}

func (t *Transmitter) markBatchTransmitted(batch *database.Batch) error {
	// Start transaction
	tx := t.db.NewTransaction()

	// Update batch to transmitted
	batch.Status = "transmitted"

	// Insert
	batch.UpdateQuery(tx)

	// Commit transaction
	return tx.Commit()
}
