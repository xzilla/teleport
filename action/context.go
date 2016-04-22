package action

import (
	"github.com/jmoiron/sqlx"
)

// Defines the execution context of actions
type Context struct {
	Tx *sqlx.Tx
	Db *sqlx.DB
}

