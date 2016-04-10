package ddlaction

import (
	"github.com/jmoiron/sqlx"
)

type Action interface {
	// Execute the given action
	Execute(tx *sqlx.Tx)
}
