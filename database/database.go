package database

import (
	"database/sql"
	"fmt"
	log "github.com/Sirupsen/logrus"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/pagarme/teleport/action"
	"github.com/pagarme/teleport/asset"
	"github.com/pagarme/teleport/config"
)

// Database definition
type Database struct {
	Name    string
	Config  config.Database
	Schemas map[string]*Schema
	Db      *sqlx.DB
}

func New(dbconf config.Database) *Database {
	return &Database{
		Name:    dbconf.Name,
		Config:  dbconf,
		Schemas: make(map[string]*Schema),
	}
}

func (db *Database) ConnectionString() string {
	s := fmt.Sprintf("host=%s user=%s password=%s dbname=%s port=%d",
		db.Config.Hostname,
		db.Config.Username,
		db.Config.Password,
		db.Config.Database,
		db.Config.Port,
	)
	for k, v := range db.Config.Options {
		s += " " + k + "=" + v
	}
	return s
}

// Open connection with database and setup internal tables
func (db *Database) Start() error {
	var err error

	db.Db, err = sqlx.Connect("postgres", db.ConnectionString())
	db.Db.SetMaxOpenConns(5)

	if err != nil {
		return err
	}

	err = db.setupTables()

	if err != nil {
		return err
	}

	err = db.RefreshSchema()

	if err != nil {
		return err
	}

	return nil
}

func (db *Database) NewTransaction() *sqlx.Tx {
	return db.Db.MustBegin()
}

// Install DDL triggers
func (db Database) InstallDDLTriggers(useEventTriggers bool) {
	var err error

	if useEventTriggers {
		err = db.installDDLEventTriggers()
	} else {
		err = db.installLDDLWatcher()
	}

	if err == nil {
		log.Infof("Installed triggers on database")
	} else {
		log.Warnf("Failed to install triggers on database: %v", err)
	}
}

// Install triggers on a source table
func (db *Database) InstallTriggers(targetExpression string) {
	// Install triggers for each table
	for _, schema := range db.Schemas {
		for _, class := range schema.Tables {
			// If class is not a table, continue...
			if class.RelationKind != "r" {
				continue
			}

			if action.IsInTargetExpression(&targetExpression, &schema.Name, &class.RelationName) {
				class.InstallTriggers()
			}
		}
	}
}

// Install source triggers using watcher
func (db *Database) installLDDLWatcher() error {
	rows, err := db.runQueryFromFile("data/sql/ddl_watcher.sql")
	rows.Close()
	return err
}

// Install source triggers using event triggers
func (db *Database) installDDLEventTriggers() error {
	rows, err := db.runQueryFromFile("data/sql/event_triggers.sql")
	rows.Close()
	return err
}

// Setup internal tables using setup script
func (db *Database) setupTables() error {
	rows, err := db.runQueryFromFile("data/sql/setup.sql")

	if err != nil {
		return err
	}

	rows.Close()
	return nil
}

// Run query on database
func (db *Database) runQuery(query string, args ...interface{}) (*sql.Rows, error) {
	return db.Db.Query(query, args...)
}

func (db *Database) selectObjs(v interface{}, query string, args ...interface{}) error {
	return db.Db.Select(v, query, args...)
}

// Open file and run query
func (db *Database) runQueryFromFile(file string) (*sql.Rows, error) {
	// Read file
	content, err := asset.Asset(file)

	if err != nil {
		return nil, err
	}

	return db.runQuery(string(content))
}
