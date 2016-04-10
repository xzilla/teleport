package action

import (
	"strings"
)

func IsInTargetExpression(targetExpression, schema, table *string) bool {
	separator := strings.Split(*targetExpression, ".")
	schemaName := separator[0]

	if *schema != schemaName {
		return false
	}

	if table == nil {
		return *schema == schemaName
	}

	prefix := strings.Split(separator[1], "*")[0]

	return *schema == schemaName && strings.HasPrefix(*table, prefix)
}
