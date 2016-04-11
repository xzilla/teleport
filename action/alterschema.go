package action

import (
	"encoding/gob"
	"fmt"
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

func (a *AlterSchema) Execute(tx *sqlx.Tx) error {
	_, err := tx.Exec(
		fmt.Sprintf("ALTER SCHEMA %s RENAME TO %s;", a.SourceName, a.TargetName),
	)

	return err
}

func (a *AlterSchema) Filter(targetExpression string) bool {
	return IsInTargetExpression(&targetExpression, &a.SourceName, nil)
}
