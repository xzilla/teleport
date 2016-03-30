package database

import (
	"database/sql"
	_ "github.com/lib/pq"
	"fmt"
)

// Database definition
type Database struct {
	Database string
	Hostname string
	Username string
	Password string
	Port     int
	db *sql.DB
}

// Open connection with database
func (db *Database) Connect() error {
	var err error

	db.db, err = sql.Open("postgres", fmt.Sprintf(
		"host=%s user=%s password=%s dbname=%s port=%d sslmode=disable",
		db.Hostname,
		db.Username,
		db.Password,
		db.Database,
		db.Port,
	))

	if err != nil {
		return err
	}

	return db.db.Ping()
}

func (db *Database) runQuery(query string, args ...interface{}) (*sql.Rows, error)  {
	return db.db.Query(query, args...)
}
