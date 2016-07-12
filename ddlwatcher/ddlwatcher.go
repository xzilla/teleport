package ddlwatcher

import (
	"github.com/pagarme/teleport/database"
	"log"
	"time"
)

type DdlWatcher struct {
	db *database.Database
}

func New(db *database.Database) *DdlWatcher {
	return &DdlWatcher{
		db: db,
	}
}

func (d *DdlWatcher) Watch(sleepTime time.Duration) {
	for {
		err := d.runWatcher()

		if err != nil {
			log.Printf("Error running DDL watcher! %v\n", err)
		}

		time.Sleep(sleepTime)
	}
}

func (d *DdlWatcher) runWatcher() error {
	// SERIALIZABLE is needed to ensure consistency when
	// fetching the schema from pg_catalog.
	_, err := d.db.Db.Exec(`
		BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE;
			SELECT teleport.ddl_watcher();
		COMMIT;
	`)

	return err
}
