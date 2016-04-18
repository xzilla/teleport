package action

import (
	"encoding/gob"
	"fmt"
	"strings"
)

type CreateTrigger struct {
	SchemaName  string
	TableName   string
	TriggerName string
	// BEFORE | AFTER | INSTEAD OF
	ExecutionOrder string
	// INSERT | UPDATE | DELETE
	Events        []string
	ProcedureName string
}

// Register type for gob
func init() {
	gob.Register(&CreateTrigger{})
}

func (a *CreateTrigger) Execute(c Context) error {
	_, err := c.Tx.Exec(
		fmt.Sprintf(
			`
				CREATE TRIGGER "%s"
				%s %s ON "%s"."%s"
				FOR EACH ROW EXECUTE PROCEDURE %s();
			`,
			a.TriggerName,
			a.ExecutionOrder,
			strings.Join(a.Events, " OR "),
			a.SchemaName,
			a.TableName,
			a.ProcedureName,
		),
	)

	return err
}

func (a *CreateTrigger) Filter(targetExpression string) bool {
	return IsInTargetExpression(&targetExpression, &a.SchemaName, &a.TableName)
}

func (a *CreateTrigger) NeedsSeparatedBatch() bool {
	return false
}
