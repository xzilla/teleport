package action

import (
	"encoding/gob"
	"regexp"
	"fmt"
)

type CreateFunction struct {
	SchemaName string
	FunctionName string
	FunctionDef string
}

var schemaDefParser *regexp.Regexp

// Register type for gob
func init() {
	gob.Register(&CreateFunction{})
	schemaDefParser = regexp.MustCompile(`([a-z_]+\.|"[^"]+"\.)`)
}

func (a *CreateFunction) getFunctionDefForTargetSchema() string {
	return schemaDefParser.ReplaceAllString(a.FunctionDef, fmt.Sprintf("%s.", a.SchemaName))
}

func (a *CreateFunction) Execute(c *Context) error {
	// _, err := c.Tx.Exec(a.replaceSchemaForDef())
	// return err
	var originalSearchPath string

	err := c.Tx.Get(&originalSearchPath, "SHOW search_path;")

	if err != nil {
		return err
	}

	_, err = c.Tx.Exec(
		fmt.Sprintf("SET search_path = %s;", a.SchemaName),
	)

	if err != nil {
		return err
	}

	_, err = c.Tx.Exec(a.getFunctionDefForTargetSchema())

	if err != nil {
		return err
	}

	_, err = c.Tx.Exec(
		fmt.Sprintf("SET search_path = %s;", originalSearchPath),
	)

	return err
}

func (a *CreateFunction) Filter(targetExpression string) bool {
	return true
}

func (a *CreateFunction) NeedsSeparatedBatch() bool {
	return false
}
