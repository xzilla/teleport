package batcher

import (
	"fmt"
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

	err =  b.db.RefreshSchema()

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
	usedEvents, _, err := b.CreateBatchesWithActions(actionsForEvent)

	if err != nil {
		return err
	}

	// All unused events should have "ignored" as status
	return b.markIgnoredEvents(usedEvents, actionsForEvent)
}

func (b *Batcher) markIgnoredEvents(usedEvents []*database.Event, actionsForEvent map[database.Event][]action.Action) error {
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

func (b *Batcher) CreateBatchesWithActions(actionsForEvent map[database.Event][]action.Action) ([]*database.Event, []*database.Batch, error) {
	usedEvents := make([]*database.Event, 0)
	batches := make([]*database.Batch, 0)

	// Create a batch for each target
	for targetName, target := range b.targets {
		targetActionsEvents := b.filterActionsForTarget(target, actionsForEvent)

		currentBatch := make([]database.Event, 0)

		// Create batch with pending items to batch
		createBatch := func() error {
			batch, err := b.createBatchWithEvents(currentBatch, targetName)

			if err != nil {
				return err
			}

			if batch != nil {
				batches = append(batches, batch)
			}

			return nil
		}

		for event, actions := range targetActionsEvents {
			for _, act := range actions {
				// Add event to used events
				usedEvents = append(usedEvents, &event)

				// Each action is a new event.
				newEvent := event
				// Encode action inside event's data
				err := newEvent.SetDataFromAction(act)

				if err != nil {
					return nil, nil, err
				}

				if act.NeedsSeparatedBatch() {
					// Create batch with pending items to batch
					if err := createBatch(); err != nil {
						return nil, nil, err
					}

					// Then create the new batch containing only this event
					currentBatch = []database.Event{newEvent}
					if err := createBatch(); err != nil {
						return nil, nil, err
					}

					// Reset currentBatch for next events
					currentBatch = make([]database.Event, 0)
				} else {
					// If action doesn't need separate batch, simply append the batch
					currentBatch = append(currentBatch, newEvent)
				}
			}
		}

		if err := createBatch(); err != nil {
			return nil, nil, err
		}
	}

	return usedEvents, batches, nil
}

func (b *Batcher) actionsForEvents(events []*database.Event) (map[database.Event][]action.Action, error) {
	actionsForEvent := make(map[database.Event][]action.Action)

	// Get actions for each event
	for _, event := range events {
		actions, err := b.actionsForEvent(*event)

		if err != nil {
			return nil, err
		}

		actionsForEvent[*event] = actions
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
	batch := database.NewBatch("db")

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
	batch.InsertQuery(tx)

	// Mark all events as belonging to this batch
	for _, event := range events {
		event.BelongsToBatch(tx, batch)
	}

	// Commit to database, returning errors
	err := tx.Commit()

	if err != nil {
		return nil, err
	}

	log.Printf("Generated new batch: %#v\n", batch)

	return batch, nil
}

func (b *Batcher) filterActionsForTarget(target *client.Client, actionsForEvent map[database.Event][]action.Action) map[database.Event][]action.Action {
	newActions := make(map[database.Event][]action.Action)

	for event, actions := range actionsForEvent {
		newActions[event] = make([]action.Action, 0)

		for _, act := range actions {
			// Filter action for target
			if act.Filter(target.TargetExpression) {
				newActions[event] = append(newActions[event], act)
			}
		}
	}

	return newActions
}

func (b *Batcher) actionsForEvent(event database.Event) ([]action.Action, error) {
	if event.Data == nil {
		return []action.Action{}, fmt.Errorf("caught event with no data!")
	}

	if event.Kind == "ddl" {
		ddl := database.NewDdl(b.db, []byte(*event.Data))

		// Update database schema
		for _, schema := range ddl.PostSchemas {
			b.db.Schemas[schema.Name] = schema
		}

		actions := ddl.Diff()
		return actions, nil
	} else if event.Kind == "dml" {
		dml := database.NewDml(b.db, &event, []byte(*event.Data))
		actions := dml.Diff()
		return actions, nil
	} else {
		act, err := event.GetActionFromData()

		if err != nil {
			return []action.Action{}, err
		}

		return []action.Action{act}, nil
	}

	return []action.Action{}, nil
}
