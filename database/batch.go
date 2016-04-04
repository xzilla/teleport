package database

import (
	"github.com/jmoiron/sqlx"
	"time"
	"log"
)

type Batch struct {
	Id     string  `db:"id" json:"id"`
	Status string  `db:"status" json:"status"`
	Data   *string `db:"data" json:"data"`
}

func NewBatch(data []byte) *Batch {
	dataStr := string(data)

	return &Batch{
		Data:   &dataStr,
		Status: "waiting_transmission",
	}
}

// Transmit batches
func (db *Database) TransmitBatches(sleepTime time.Duration) {
	for {
		_, err := db.GetBatches("waiting_transmission")

		if err != nil {
			log.Printf("Error fetching batches for transmission! %v\n", err)
		} else {
			// for _, batch := range batches {
			// 	// batch.Transmit()
			// }
		}

		time.Sleep(sleepTime)
	}
}

func (db *Database) GetBatches(status string) ([]Batch, error) {
	var batches []Batch
	err := db.selectObjs(&batches, "SELECT * FROM teleport.batch WHERE status = $1 ORDER BY id ASC;", status)
	return batches, err
}

func (b *Batch) InsertQuery(tx *sqlx.Tx) {
	tx.MustExec(
		"INSERT INTO teleport.batch (status, data) VALUES ($1, $2)",
		b.Status,
		b.Data,
	)
}

func (b *Batch) UpdateQuery(tx *sqlx.Tx) {
	tx.MustExec(
		"UPDATE teleport.batch SET status = $1 WHERE id = $2",
		b.Status,
		b.Id,
	)
}
