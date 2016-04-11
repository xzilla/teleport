package batcher

import (
	"bytes"
	"encoding/base64"
	"encoding/gob"
	"github.com/pagarme/teleport/action"
	"github.com/pagarme/teleport/client"
	"github.com/pagarme/teleport/database"
	"log"
	"time"
)

type Batcher struct {
	db      *database.Database
	targets map[string]*client.Client
}

func New(db *database.Database, targets map[string]*client.Client) *Batcher {
	return &Batcher{
		db:      db,
		targets: targets,
	}
}

// Every sleepTime interval, create a batch with unbatched events
func (b *Batcher) Watch(sleepTime time.Duration) {
	for {
		err := b.createBatches()

		if err != nil {
			log.Printf("Error creating batch! %v\n", err)
		}

		time.Sleep(sleepTime)
	}
}

// Group all events 'waiting_batch' and create a batch with them.
func (b *Batcher) createBatches() error {
	// Get events waiting replication
	events, err := b.db.GetEvents("waiting_batch")

	if err != nil {
		return err
	}

	// Stop if there are no events
	if len(events) == 0 {
		return nil
	}

	// Get actions for each event
	actionsForEvent, err := b.actionsForEvents(events)

	if err != nil {
		return err
	}

	// Create batches for each target with the given targets/actions
	usedEvents, err := b.createBatchesForTargets(b.targets, actionsForEvent)

	if err != nil {
		return err
	}

	// All unused events should have "ignored" as status
	return b.markIgnoredEvents(usedEvents, actionsForEvent)
}

func (b *Batcher) markIgnoredEvents(usedEvents []database.Event, actionsForEvent map[database.Event][]action.Action) error {
	// Mark unused events as ignored
	tx := b.db.NewTransaction()

	for event, _ := range actionsForEvent {
		eventUsed := false

		for _, usedEvent := range usedEvents {
			if usedEvent.Id == event.Id {
				eventUsed = true
				break
			}
		}

		if !eventUsed {
			event.Status = "ignored"
			event.UpdateQuery(tx)
		}
	}

	return tx.Commit()
}

func (b *Batcher) createBatchesForTargets(targets map[string]*client.Client, actionsForEvent map[database.Event][]action.Action) ([]database.Event, error) {
	usedEvents := make([]database.Event, 0)

	// Create a batch for each target
	for targetName, target := range b.targets {
		events, err := b.eventsForTarget(target, actionsForEvent)
		usedEvents = append(usedEvents, events...)

		if err != nil {
			return nil, err
		}

		_, err = b.createBatchWithEvents(events, targetName)

		if err != nil {
			return nil, err
		}
	}

	return usedEvents, nil
}

func (b *Batcher) actionsForEvents(events []database.Event) (map[database.Event][]action.Action, error) {
	actionsForEvent := make(map[database.Event][]action.Action)

	// Get actions for each event
	for _, event := range events {
		actions, err := b.actionsForEvent(event)

		if err != nil {
			return nil, err
		}

		actionsForEvent[event] = actions
	}

	return actionsForEvent, nil
}

func (b *Batcher) createBatchWithEvents(events []database.Event, targetName string) (*database.Batch, error) {
	// Don't create batch if there are no events
	if len(events) == 0 {
		return nil, nil
	}

	// Start a transaction
	tx := b.db.NewTransaction()

	// Allocate a new batch
	batch := database.NewBatch()

	// Set events
	batch.SetEvents(events)

	// Mark events as batched
	for _, event := range events {
		event.Status = "batched"
		event.UpdateQuery(tx)
	}

	// Set source and target
	batch.Source = b.db.Name
	batch.Target = targetName

	// Insert batch
	err2 := batch.InsertQuery(tx)
	log.Printf("error inserting batch query: %v\n", err2)

	// Mark all events as belonging to this batch
	for _, event := range events {
		event.BelongsToBatch(tx, batch)
	}

	// Commit to database, returning errors
	err := tx.Commit()

	if err != nil {
		return nil, err
	}

	log.Printf("Generated new batch: %v\n", batch)

	return batch, nil
}

func (b *Batcher) eventsForTarget(target *client.Client, actionsForEvent map[database.Event][]action.Action) ([]database.Event, error) {
	events := make([]database.Event, 0)

	for event, actions := range actionsForEvent {
		for _, action := range actions {
			// Filter action for target
			if action.Filter(target.TargetExpression) {
				// Each action is a new event.
				newEvent := event

				// Encode action into event data using gob
				var buf bytes.Buffer
				encoder := gob.NewEncoder(&buf)
				err := encoder.Encode(&action)

				if err != nil {
					return nil, err
				}

				// Update event data
				encodedData := base64.StdEncoding.EncodeToString(buf.Bytes())
				newEvent.Data = &encodedData

				events = append(events, newEvent)
			}
		}
	}

	return events, nil
}

func (b *Batcher) actionsForEvent(event database.Event) ([]action.Action, error) {
	if event.Kind == "ddl" {
		ddl := database.NewDdl([]byte(*event.Data))
		actions := ddl.Diff()
		return actions, nil
	} else if event.Kind == "dml" {
		// Implement DML processor
	}

	return []action.Action{}, nil
}
