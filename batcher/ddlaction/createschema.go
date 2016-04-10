package ddlaction

import (
	"github.com/jmoiron/sqlx"
	"encoding/gob"
)

type CreateSchema struct {
	SchemaName string
}

// Register type for gob
func init() {
	gob.Register(&CreateSchema{})
}

func (a *CreateSchema) Execute(tx *sqlx.Tx) {
	tx.MustExec(
		"CREATE SCHEMA $1;",
		a.SchemaName,
	)
}
