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

// Open connection with database and setup internal tables
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

	// Ping database to check for connetivity
	err = db.db.Ping()
	if err != nil {
		return err
	}

	return db.setupTables()
}

// Setup internal tables using setup script
func (db *Database) setupTables() error {
	_, err := db.runQueryFromFile("database/sql/setup.sql")
	return err
}
