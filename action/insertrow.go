package action

import (
	"encoding/gob"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

type InsertRow struct {
	SchemaName string
	TableName  string
	Rows       []Row
}

// Register type for gob
func init() {
	gob.Register(&InsertRow{})
	gob.Register(&time.Time{})
}

func (a *InsertRow) Execute(c Context) error {
	escapedCols := make([]string, 0)
	escapedRows := make([]string, 0)
	values := make([]interface{}, 0)

	for i, row := range a.Rows {
		escapedCols = append(escapedCols, fmt.Sprintf("\"%s\"", row.Column.Name))
		escapedRows = append(escapedRows, fmt.Sprintf("$%d::%s\"%s\"", i+1, row.Column.GetTypeSchemaStr(a.SchemaName), row.Column.Type))

		// Marshall JSON objects as pg driver does not support it
		if obj, ok := row.Value.(*map[string]interface{}); ok {
			jsonStr, err := json.Marshal(obj)

			if err != nil {
				return err
			}

			values = append(values, string(jsonStr))
		} else {
			values = append(values, row.Value)
		}
	}

	_, err := c.Tx.Exec(
		fmt.Sprintf(
			`INSERT INTO "%s"."%s" (%s) VALUES (%s);`,
			a.SchemaName,
			a.TableName,
			strings.Join(escapedCols, ","),
			strings.Join(escapedRows, ","),
		),
		values...,
	)

	return err
}

func (a *InsertRow) Filter(targetExpression string) bool {
	return IsInTargetExpression(&targetExpression, &a.SchemaName, &a.TableName)
}

func (a *InsertRow) NeedsSeparatedBatch() bool {
	return false
}
