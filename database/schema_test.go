package database

import (
	"github.com/pagarme/teleport/action"
	"github.com/pagarme/teleport/batcher/ddldiff"
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

func TestDiffCreateSchema(t *testing.T) {
	var pre ddldiff.Diffable
	var post *Schema

	// Test creating a Schema
	pre = nil
	post = &Schema{
		"1234",
		"test_schema",
		[]*Class{},
	}

	actions := post.Diff(pre)

	if len(actions) != 1 {
		t.Errorf("actions => %d, want %d", len(actions), 1)
	}

	createAction, ok := actions[0].(*action.CreateSchema)

	if !ok {
		t.Errorf("action is not CreateSchema")
	}

	if createAction.SchemaName != post.Name {
		t.Errorf("create action schema name => %s, want %s", createAction.SchemaName, post.Name)
	}
}

func TestDiffRenameSchema(t *testing.T) {
	var pre *Schema
	var post *Schema

	// Test creating a Schema
	pre = &Schema{
		"1234",
		"test_schema",
		[]*Class{},
	}
	post = &Schema{
		"1234",
		"test_schema_renamed",
		[]*Class{},
	}

	actions := post.Diff(pre)

	if len(actions) != 1 {
		t.Errorf("actions => %d, want %d", len(actions), 1)
	}

	renameAction, ok := actions[0].(*action.AlterSchema)

	if !ok {
		t.Errorf("action is not AlterSchema")
	}

	if renameAction.SourceName != pre.Name {
		t.Errorf("rename action source name => %s, want %s", renameAction.SourceName, pre.Name)
	}

	if renameAction.TargetName != post.Name {
		t.Errorf("rename action target name => %s, want %s", renameAction.TargetName, post.Name)
	}
}

func TestSchemaChildren(t *testing.T) {
	classes := []*Class{
		&Class{
			"567",
			"t",
			"test_table",
			[]*Attribute{},
			nil,
		},
	}

	schema := &Schema{
		"1234",
		"test_schema",
		classes,
	}

	children := schema.Children()

	if len(children) != 1 {
		t.Errorf("children => %d, want %d", len(children), 1)
	}

	for i, child := range children {
		if child != classes[i] {
			t.Errorf("child %i => %v, want %v", i, child, classes[i])
		}
	}
}

func TestSchemaDrop(t *testing.T) {
	var schema *Schema

	schema = &Schema{
		"1234",
		"test_schema",
		[]*Class{},
	}

	actions := schema.Drop()

	if len(actions) != 1 {
		t.Errorf("actions => %d, want %d", len(actions), 1)
	}

	dropAction, ok := actions[0].(*action.DropSchema)

	if !ok {
		t.Errorf("action is not DropSchema")
	}

	if dropAction.SchemaName != schema.Name {
		t.Errorf("drop action schema name => %s, want %s", dropAction.SchemaName, schema.Name)
	}
}

func TestSchemaIsEqual(t *testing.T) {
	var pre *Schema
	var post *Schema

	pre = &Schema{
		"1234",
		"test_schema",
		[]*Class{},
	}
	post = &Schema{
		"1234",
		"test_schema_renamed",
		[]*Class{},
	}

	if !post.IsEqual(pre) {
		t.Errorf("expect schemas to be equal")
	}

	post.Name = pre.Name
	post.Oid = "1235"

	if post.IsEqual(pre) {
		t.Errorf("expect schemas not to be equal")
	}
}
