package action

import (
	"encoding/gob"
	"github.com/jmoiron/sqlx"
)

type AlterSchema struct {
	SourceName string
	TargetName string
}

// Register type for gob
func init() {
	gob.Register(&AlterSchema{})
}

func (a *AlterSchema) Execute(tx *sqlx.Tx) {
	tx.MustExec(
		"ALTER SCHEMA $1 RENAME TO $2;",
		a.SourceName,
		a.TargetName,
	)
}

func (a *AlterSchema) Filter(targetExpression string) bool {
	return IsInTargetExpression(&targetExpression, &a.SourceName, nil)
}
