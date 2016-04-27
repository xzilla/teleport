package action

import (
	"fmt"
)

type Column struct {
	Name         string
	Type         string
	IsNativeType bool
}

func (c *Column) GetTypeSchemaStr(schema string) string {
	if !c.IsNativeType {
		return fmt.Sprintf("\"%s\".", schema)
	}

	return ""
}
