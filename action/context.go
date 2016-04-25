package action

import (
	"github.com/jmoiron/sqlx"
	"crypto/md5"
)

// Defines the execution context of actions
type Context struct {
	Tx *sqlx.Tx
	Db *sqlx.DB
	stmtCache map[string]*sqlx.Stmt
}

func NewContext(tx *sqlx.Tx, db *sqlx.DB) *Context {
	return &Context{
		tx,
		db,
		make(map[string]*sqlx.Stmt),
	}
}

func (c *Context) getHash(str string) string {
	h := md5.New()
	return string(h.Sum([]byte(str)))
}

func (c *Context) GetPreparedStatement(statement string) (*sqlx.Stmt, error) {
	hash := c.getHash(statement)

	if stmt, ok := c.stmtCache[hash]; ok {
		return stmt, nil
	} else {
		stmt, err := c.Tx.Preparex(statement)

		if err != nil {
			return nil, err
		}

		c.stmtCache[hash] = stmt

		return stmt, nil
	}

	return nil, nil
}
