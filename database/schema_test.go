package database

import (
	"testing"
)

func TestParseSchema(t *testing.T) {
	schemaStr := `[
		{"oid":"2200","schema_name":"public","owner_id":"10","classes":
			[{"oid":"16443","namespace_oid":"2200","relation_kind":"r","relation_name":"test_table","attributes":
				[{"class_oid":"16443","attr_name":"id","attr_num":1,"type_name":"int4","type_oid":"23"}]
			}]
		},
		{"oid":"11320","schema_name":"pg_temp_1","owner_id":"10","classes":null},
		{"oid":"11321","schema_name":"pg_toast_temp_1","owner_id":"10","classes":null}
	]`

	schemas, err := ParseSchema(schemaStr)

	if err != nil {
		t.Errorf("parse schema returned error: %v", err)
	}

	if len(schemas) != 3 {
		t.Errorf("schemas => %d, want %d", len(schemas), 3)
	}

	if schemas[0].Name != "public" {
		t.Errorf("schema name => %s, want %s", schemas[0].Name, "public")
	}

	if len(schemas[0].Classes) != 1 {
		t.Errorf("schema classes => %d, want %d", len(schemas[0].Classes), 1)
	}

	if len(schemas[0].Classes[0].Attributes) != 1 {
		t.Errorf("schema class attributes => %d, want %d", len(schemas[0].Classes[0].Attributes), 1)
	}

	// Validate parent references
	for _, schema := range schemas {
		for _, class := range schema.Classes {
			if class.Schema != schema {
				t.Errorf("class doesn't point to parent schema!")
			}

			for _, attr := range class.Attributes {
				if attr.Class != class {
					t.Errorf("attr doesn't point to parent class!")
				}
			}
		}
	}
}
