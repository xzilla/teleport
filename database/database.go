package database

import (
	"database/sql"
	"fmt"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"io/ioutil"
	"os"
)

// Database definition
type Database struct {
	Database string
	Hostname string
	Username string
	Password string
	Port     int
	Schemas  map[string]*Schema
	db       *sqlx.DB
}

// Open connection with database and setup internal tables
func (db *Database) Start() error {
	var err error

	db.db, err = sqlx.Connect("postgres", fmt.Sprintf(
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

	// Initialize schema map
	db.Schemas = make(map[string]*Schema)

	return db.setupTables()
}

// Install triggers on a source table
func (db *Database) InstallTriggers(sourceTables string) error {
	// Get tables for sourceTables string
	tables, err := db.tablesForSourceTables(sourceTables)

	if err != nil {
		return err
	}

	// Install triggers for each table/schema
	for _, table := range tables {
		if !table.Schema.HasTriggers {
			table.Schema.InstallTriggers()
		}

		table.InstallTriggers()

		fmt.Printf("Tables! %v\n", table.Name)
	}

	return nil
}

// Setup internal tables using setup script
func (db *Database) setupTables() error {
	_, err := db.runQueryFromFile("database/sql/setup.sql")
	return err
}

// Run query on database
func (db *Database) runQuery(query string, args ...interface{}) (*sql.Rows, error) {
	return db.db.Query(query, args...)
}

func (db *Database) selectObjs(v interface{}, query string, args ...interface{}) error {
	return db.db.Select(v, query, args...)
}

// Open file and run query
func (db *Database) runQueryFromFile(path string) (*sql.Rows, error) {
	// Get current directory
	pwd, err := os.Getwd()

	if err != nil {
		return nil, err
	}

	// Read file
	content, err := ioutil.ReadFile(pwd + "/" + path)

	if err != nil {
		return nil, err
	}

	return db.runQuery(string(content))
}
