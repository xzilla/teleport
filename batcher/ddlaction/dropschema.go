package ddlaction

import (
	"github.com/jmoiron/sqlx"
	"encoding/gob"
)

type DropSchema struct {
	SchemaName string
}

// Register type for gob
func init() {
	gob.Register(&DropSchema{})
}

func (a *DropSchema) Execute(tx *sqlx.Tx) {
	tx.MustExec(
		"DROP SCHEMA $1;",
		a.SchemaName,
	)
}
