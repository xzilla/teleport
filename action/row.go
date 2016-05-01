package action

import (
	"encoding/json"
)

type Row struct {
	Value  interface{}
	Column Column
}

type Rows []Row

func (r *Row) GetValue() interface{} {
	// pq cannot handle nested JSON objects
	if obj, ok := r.Value.(*map[string]interface{}); ok {
		jsonStr, err := json.Marshal(obj)

		if err != nil {
			return err
		}

		return string(jsonStr)
	}

	// text/varchar are converted to slice of bytes.
	// Convert back to string...
	if bytea, ok := r.Value.([]byte); ok {
		return string(bytea)
	}

	return r.Value
}

// Implement Interface
func (slice Rows) Len() int {
	return len(slice)
}

func (slice Rows) Less(i, j int) bool {
	return slice[i].Column.Name < slice[j].Column.Name
}

func (slice Rows) Swap(i, j int) {
	slice[i], slice[j] = slice[j], slice[i]
}
