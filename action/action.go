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
	// Returns whether the action needs to be batched separately to ensure
	// consistency. Some actions cannot run inside transactions, thus must have
	// a separate batch to ensure apply order and consistency.
	NeedsSeparatedBatch() bool
}
