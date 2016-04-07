package database

import (
	"encoding/json"
)

type Ddl struct {
	PreSchemas []Schema `json:"pre"`
	PostSchemas []Schema `json:"post"`
}

func NewDdl(data []byte) *Ddl {
	var ddl Ddl
	err := json.Unmarshal(data, &ddl)

	if err != nil {
		panic(err)
	}

	return &ddl
}
