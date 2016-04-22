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
	_, err := d.db.Db.Exec(`
		SELECT teleport_ddl_watcher();
	`)

	return err
}
