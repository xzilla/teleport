package transmitter

import (
	"fmt"
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
		batches, err := t.db.GetBatches("waiting_transmission", "")

		if err != nil {
			log.Printf("Error fetching batches for transmission! %v\n", err)
		} else {
			for _, batch := range batches {
				err := t.transmitBatch(batch)

				if err != nil {
					log.Printf("Error transmitting batch! %v\n", err)
				}
			}
		}

		batches, err = t.db.GetBatches("transmitted", "waiting_transmission")

		if err != nil {
			log.Printf("Error fetching batches data for transmission! %v\n", err)
		} else {
			for _, batch := range batches {
				err := t.transmitBatchData(batch)

				if err != nil {
					log.Printf("Error transmitting batch! %v\n", err)
				}
			}
		}

		time.Sleep(sleepTime)
	}
}

// Transmit a batch to a target
func (t *Transmitter) transmitBatch(batch *database.Batch) error {
	client := t.clients[batch.Target]

	if client == nil {
		return fmt.Errorf("could not find client for target '%s'", batch.Target)
	}

	_, err := client.SendRequest("/batches", batch)

	if err != nil {
		return err
	}

	log.Printf("Transmitted batch: %#v", batch)

	return t.markBatchTransmitted(batch, false)
}

// Transmit a batch data to a target
func (t *Transmitter) transmitBatchData(batch *database.Batch) error {
	client := t.clients[batch.Target]

	if client == nil {
		return fmt.Errorf("could not find client for target '%s'", batch.Target)
	}

	file, err := batch.GetFile()

	if err != nil {
		return err
	}

	log.Printf("Transmitting batch data: %#v", batch)

	_, err = client.SendFile(
		fmt.Sprintf("/batches/%s", batch.Id),
		"data",
		file,
	)

	defer file.Close()

	if err != nil {
		return err
	}

	log.Printf("Transmitted batch data: %#v", batch)

	return t.markBatchTransmitted(batch, true)
}

func (t *Transmitter) markBatchTransmitted(batch *database.Batch, updateData bool) error {
	// Start transaction
	tx := t.db.NewTransaction()

	// Update batch to transmitted
	batch.Status = "transmitted"
	batch.DataStatus = ""

	if updateData {
		batch.DataStatus = "transmitted"
	}

	err := batch.UpdateQuery(tx)

	if err != nil {
		return err
	}

	// Commit transaction
	return tx.Commit()
}
