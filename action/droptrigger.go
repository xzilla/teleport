package action

import (
	"encoding/gob"
	"fmt"
)

type DropTrigger struct {
	SchemaName  string
	TableName   string
	TriggerName string
}

// Register type for gob
func init() {
	gob.Register(&DropTrigger{})
}

func (a *DropTrigger) Execute(c Context) error {
	_, err := c.Tx.Exec(
		fmt.Sprintf(
			`DROP TRIGGER IF EXISTS "%s" ON "%s"."%s";`,
			a.TriggerName,
			a.SchemaName,
			a.TableName,
		),
	)

	return err
}

func (a *DropTrigger) Filter(targetExpression string) bool {
	return IsInTargetExpression(&targetExpression, &a.SchemaName, &a.TableName)
}
