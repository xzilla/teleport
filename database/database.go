package database

import (
	"database/sql"
	"fmt"
	_ "github.com/lib/pq"
	"io/ioutil"
	"os"
	"strings"
)

// Database definition
type Database struct {
	Database string
	Hostname string
	Username string
	Password string
	Port     int
	Schemas  map[string]*Schema
	db       *sql.DB
}

// Open connection with database and setup internal tables
func (db *Database) Start() error {
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

	db.Schemas = make(map[string]*Schema)

	return db.setupTables()
}

// Install triggers on a a
func (db *Database) InstallTriggers(sourceTables string) error {
	schemaName := strings.Split(sourceTables, ".")[0]

	// Fetch schema from database if it's not already loaded
	if db.Schemas[schemaName] == nil {
		var err error
		db.Schemas[schemaName], err = db.fetchSchema(schemaName)

		if err != nil {
			return err
		}
	}

	schema := db.Schemas[schemaName]

	_ = schema

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
