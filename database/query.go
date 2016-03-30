package database

import (
	"database/sql"
	"io/ioutil"
	"os"
)

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
