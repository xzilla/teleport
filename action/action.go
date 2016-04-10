package action

import (
	"github.com/jmoiron/sqlx"
)

type Action interface {
	// Execute the given action
	Execute(tx *sqlx.Tx)
	// Returns whether current action should be executed
	// for a targetExpression
	Filter(targetExpression string) bool
}
