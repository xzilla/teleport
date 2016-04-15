package action

import (
	"encoding/gob"
	"fmt"
	"strings"
)

type CreateType struct {
	SchemaName string
	TypeName   string
	Enums      []string
}

// Register type for gob
func init() {
	gob.Register(&CreateType{})
}

func (a *CreateType) Execute(c Context) error {
	escapedEnums := make([]string, 0)

	for _, enum := range a.Enums {
		escapedEnums = append(escapedEnums, fmt.Sprintf("'%s'", enum))
	}

	_, err := c.Tx.Exec(
		fmt.Sprintf(
			"CREATE TYPE \"%s\".\"%s\" AS ENUM (%s);",
			a.SchemaName,
			a.TypeName,
			strings.Join(escapedEnums, ","),
		),
	)

	return err
}

func (a *CreateType) Filter(targetExpression string) bool {
	return IsInTargetExpression(&targetExpression, &a.SchemaName, nil)
}
