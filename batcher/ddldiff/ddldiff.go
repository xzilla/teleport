package ddldiff

import (
	"github.com/pagarme/teleport/database"
	"encoding/json"
)

type DdlDiff struct {
	PreSchemas database.Schemas `json:"pre"`
	PostSchemas database.Schemas `json:"post"`
}

func New(sourceEvent database.Event) *DdlDiff {
	var ddlDiff DdlDiff
	err := json.Unmarshal([]byte(*sourceEvent.Data), &ddlDiff)

	if err != nil {
		panic(err)
	}

	return &ddlDiff
}

func (d *DdlDiff) GetDiff() {
}
