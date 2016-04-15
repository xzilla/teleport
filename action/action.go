package action

import (
	"github.com/jmoiron/sqlx"
)

// Defines the execution context of actions
type Context struct {
	Tx *sqlx.Tx
	Db *sqlx.DB
}

type Action interface {
	// Execute the given action
	Execute(c Context) error
	// Returns whether current action should be executed
	// for a targetExpression
	Filter(targetExpression string) bool
}
