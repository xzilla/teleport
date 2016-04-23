package vacuum

import (
	"github.com/pagarme/teleport/database"
	"log"
	"time"
)

type Vacuum struct {
	db *database.Database
}

func New(db *database.Database) *Vacuum {
	return &Vacuum{
		db: db,
	}
}

func (v *Vacuum) Watch(sleepTime time.Duration) {
	for {
		err := v.clean()

		if err != nil {
			log.Printf("Error vacuum cleaning! %v\n", err)
		}

		time.Sleep(sleepTime)
	}
}

func (v *Vacuum) clean() error {
	err := v.cleanBatches()

	if err != nil {
		return err
	}

	return v.cleanEvents()
}

func (v *Vacuum) cleanBatches() error {
	appliedBatches, err := v.db.GetBatches("applied", "")

	if err != nil {
		return err
	}

	transmittedBatches, err := v.db.GetBatches("transmitted", "transmitted")

	if err != nil {
		return err
	}

	for _, batch := range append(appliedBatches, transmittedBatches...) {
		if batch.StorageType == "fs" {
			err := batch.PurgeData()
			if err != nil {
				return err
			}
		}

		v.db.Db.Exec(`
			DELETE FROM teleport.batch WHERE id = $1;
		`, batch.Id)
	}

	return nil
}

func (v *Vacuum) cleanEvents() error {
	_, err := v.db.Db.Exec(`
		DELETE FROM teleport.event WHERE status IN ('batched', 'ignored');
	`)

	return err
}
