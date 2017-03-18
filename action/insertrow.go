package action

import (
	"encoding/gob"
	"encoding/json"
	"fmt"
	"github.com/lib/pq"
	"strings"
	"time"
)

type InsertRow struct {
	SchemaName     string
	TableName      string
	PrimaryKeyName string
	Rows           Rows
	BulkInsert     bool
}

// Register type for gob
func init() {
	gob.Register(&InsertRow{})
	gob.Register(&time.Time{})
}

type RowData struct {
	EscapedCols   []string
	EscapedRows   []string
	Values        []interface{}
	PrimaryKeyRow Row
	SchemaName    string
	TableName     string
	Rows          Rows
}

func (r RowData) SavePointQuery() string {
	return fmt.Sprintf(
		`SAVEPOINT "%s%s";`,
		r.SchemaName,
		r.TableName,
	)
}

func (r RowData) RollbackSavePointQuery() string {
	return fmt.Sprintf(
		`ROLLBACK TO SAVEPOINT "%s%s";`,
		r.SchemaName,
		r.TableName,
	)
}

func (r RowData) InsertValuesQuery() string {
	return fmt.Sprintf(
		`INSERT INTO "%s"."%s" (%s) VALUES (%s);`,
		r.SchemaName,
		r.TableName,
		strings.Join(r.EscapedCols, ","),
		strings.Join(r.EscapedRows, ","),
	)
}

func (r RowData) InsertOnConstraintUpdateQuery() string {
	updateStatements := make([]string, 0)

	for _, col := range r.EscapedCols {
		statement := fmt.Sprintf(`%s = EXCLUDED.%s`, col, col)
		updateStatements = append(updateStatements, statement)
	}

	return fmt.Sprintf(`
		INSERT INTO "%s"."%s" (%s) VALUES (%s)
		ON CONFLICT (%s) DO UPDATE
		SET %s;`,
		r.SchemaName,
		r.TableName,
		strings.Join(r.EscapedCols, ","),
		strings.Join(r.EscapedRows, ","),
		fmt.Sprintf("\"%s\"", r.PrimaryKeyRow.Column.Name),
		strings.Join(updateStatements, ","),
	)
}

func (r RowData) ReleaseSavePointQuery() string {
	return fmt.Sprintf(
		`RELEASE SAVEPOINT "%s%s";`,
		r.SchemaName,
		r.TableName,
	)
}

func (a *InsertRow) Execute(c *Context) error {
	if a.BulkInsert {
		escapedCols := make([]string, 0)
		values := make([]interface{}, 0)

		for _, row := range a.Rows {
			escapedCols = append(escapedCols, row.Column.Name)
			values = append(values, row.GetValue())
		}

		stmt, err := c.GetPreparedStatement(
			pq.CopyInSchema(a.SchemaName, a.TableName, escapedCols...),
		)

		if err != nil {
			return err
		}

		_, err = stmt.Exec(values...)

		if err != nil {
			return err
		}
	} else {
		// Perform a single insert (upsert)
		data := &RowData{}
		data.SchemaName = a.SchemaName
		data.TableName = a.TableName
		data.EscapedCols = make([]string, 0)
		data.EscapedRows = make([]string, 0)
		data.Values = make([]interface{}, 0)
		data.Rows = a.Rows

		for i, row := range a.Rows {
			data.EscapedCols = append(data.EscapedCols, fmt.Sprintf("\"%s\"", row.Column.Name))
			data.EscapedRows = append(data.EscapedRows, fmt.Sprintf("$%d::%s\"%s\"", i+1, row.Column.GetTypeSchemaStr(a.SchemaName), row.Column.Type))

			// Marshall JSON objects as pg driver does not support it
			if obj, ok := row.Value.(*map[string]interface{}); ok {
				jsonStr, err := json.Marshal(obj)

				if err != nil {
					return err
				}

				data.Values = append(data.Values, string(jsonStr))
			} else {
				data.Values = append(data.Values, row.Value)
			}

			if row.Column.Name == a.PrimaryKeyName {
				data.PrimaryKeyRow = row
			}
		}

		return chosenUpserter.upsert(data, c)
	}

	return nil
}

var chosenUpserter Upserter

func SetUpsertMethod(upserter Upserter) {
	chosenUpserter = upserter
}

type Upserter interface {
	upsert(*RowData, *Context) error
}

type FallbackUpserter struct{}

func (f FallbackUpserter) upsert(data *RowData, c *Context) error {
	// Save transaction prior to inserting to rollback
	// if INSERT fails, so a UPDATE can be tried
	_, err := c.Tx.Exec(data.SavePointQuery())

	if err != nil {
		return err
	}

	_, err = c.Tx.Exec(
		data.InsertValuesQuery(),
		data.Values...,
	)

	// Try to UPDATE (upsert) if INSERT fails...
	if err != nil {
		// Rollback to SAVEPOINT
		_, err = c.Tx.Exec(data.RollbackSavePointQuery())

		if err != nil {
			return err
		}

		updateAction := &UpdateRow{
			data.SchemaName,
			data.TableName,
			data.PrimaryKeyRow,
			data.Rows,
		}

		return updateAction.Execute(c)
	} else {
		// Release SAVEPOINT to avoid "out of shared memory"
		_, err := c.Tx.Exec(data.ReleaseSavePointQuery())
		return err
	}
}

type OnConflictUpserter struct{}

func (f OnConflictUpserter) upsert(data *RowData, c *Context) error {
	_, err := c.Tx.Exec(
		data.InsertOnConstraintUpdateQuery(),
		data.Values...,
	)
	return err
}

func (a *InsertRow) Filter(targetExpression string) bool {
	return IsInTargetExpression(&targetExpression, &a.SchemaName, &a.TableName)
}

func (a *InsertRow) NeedsSeparatedBatch() bool {
	return false
}
