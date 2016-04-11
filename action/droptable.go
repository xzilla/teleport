package action

import (
	"encoding/gob"
	"fmt"
	"github.com/jmoiron/sqlx"
)

type DropTable struct {
	SchemaName string
	TableName  string
}

// Register type for gob
func init() {
	gob.Register(&DropTable{})
}

func (a *DropTable) Execute(tx *sqlx.Tx) error {
	_, err := tx.Exec(
		fmt.Sprintf("DROP TABLE %s.%s;", a.SchemaName, a.TableName),
	)

	return err
}

func (a *DropTable) Filter(targetExpression string) bool {
	return IsInTargetExpression(&targetExpression, &a.SchemaName, &a.TableName)
}
