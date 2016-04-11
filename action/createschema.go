package action

import (
	"encoding/gob"
	"fmt"
	"github.com/jmoiron/sqlx"
)

type CreateSchema struct {
	SchemaName string
}

// Register type for gob
func init() {
	gob.Register(&CreateSchema{})
}

func (a *CreateSchema) Execute(tx *sqlx.Tx) error {
	_, err := tx.Exec(
		fmt.Sprintf("CREATE SCHEMA %s;", a.SchemaName),
	)

	return err
}

func (a *CreateSchema) Filter(targetExpression string) bool {
	return IsInTargetExpression(&targetExpression, &a.SchemaName, nil)
}
