package database

import (
	"github.com/jmoiron/sqlx"
)

type Batch struct {
	Id     string  `db:"id" json:"id"`
	Status string  `db:"status" json:"status"`
	Source string  `db:"source" json:"source"`
	Target string  `db:"target" json:"target"`
	Data   *string `db:"data" json:"data"`
}

func NewBatch(data []byte) *Batch {
	dataStr := string(data)

	return &Batch{
		Data:   &dataStr,
		Status: "waiting_transmission",
	}
}

func (db *Database) GetBatches(status string) ([]Batch, error) {
	var batches []Batch
	err := db.selectObjs(&batches, "SELECT * FROM teleport.batch WHERE status = $1 ORDER BY id ASC;", status)
	return batches, err
}

func (b *Batch) InsertQuery(tx *sqlx.Tx) {
	args := make([]interface{}, 0)
	var query string

	// If there's no id, insert without id
	if b.Id == "" {
		query = "INSERT INTO teleport.batch (status, data, source, target) VALUES ($1, $2, $3, $4)"
	} else {
		query = "INSERT INTO teleport.batch (id, status, data, source, target) VALUES ($1, $2, $3, $4, $5)"
		args = append(args, b.Id)
	}

	args = append(args,
		b.Status,
		b.Data,
		b.Source,
		b.Target,
	)

	tx.MustExec(query, args...)
}

func (b *Batch) UpdateQuery(tx *sqlx.Tx) {
	tx.MustExec(
		"UPDATE teleport.batch SET status = $1 WHERE id = $2",
		b.Status,
		b.Id,
	)
}
