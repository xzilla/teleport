package action

import (
	"encoding/gob"
	"fmt"
	"github.com/jmoiron/sqlx"
)

type DropSchema struct {
	SchemaName string
}

// Register type for gob
func init() {
	gob.Register(&DropSchema{})
}

func (a *DropSchema) Execute(tx *sqlx.Tx) error {
	_, err := tx.Exec(
		fmt.Sprintf("DROP SCHEMA %s;", a.SchemaName),
	)

	return err
}

func (a *DropSchema) Filter(targetExpression string) bool {
	return IsInTargetExpression(&targetExpression, &a.SchemaName, nil)
}
