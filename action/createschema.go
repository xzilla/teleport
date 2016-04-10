package action

import (
	"encoding/gob"
	"github.com/jmoiron/sqlx"
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

func (a *CreateSchema) Filter(targetExpression string) bool {
	return IsInTargetExpression(&targetExpression, &a.SchemaName, nil)
}
