package database

import (
	"strings"
)

func ParseTargetExpression(targetExpression string) (string, string) {
	separator := strings.Split(targetExpression, ".")
	return separator[0], separator[1]
}
