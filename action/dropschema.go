package action

import (
	"encoding/gob"
	"github.com/jmoiron/sqlx"
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

func (a *DropSchema) Filter(targetExpression string) bool {
	return IsInTargetExpression(&targetExpression, &a.SchemaName, nil)
}
