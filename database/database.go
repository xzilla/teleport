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
		"host=%s user=%s password=%s dbname=%s sslmode=verify-full",
		db.Hostname,
		db.Username,
		db.Password,
		db.Database,
	))

	if err != nil {
		return err
	}

	return db.db.Ping()
}

func (db *Database) RunQuery(query string, args ...interface{}) (*sql.Rows, error)  {
	return db.db.Query(query, args...)
}
