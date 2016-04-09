package ddldiff

import (
	"testing"
)

// Foo implements Diffable
type Foo struct {
	Name string
	Id int
	Bars []*Bar
}

type Bar struct {
	Name string
	Id int
}

func NewFoo(name string, id int, bars []*Bar) *Foo {
	return &Foo{name, id, bars}
}

func NewBar(name string, id int) *Bar {
	return &Bar{name, id}
}

func (post *Foo) Diff(other Diffable) []Action {
	if other == nil {
		return []Action{
			Action{
				"CREATE",
				"OBJ",
				post,
			},
		}
	} else {
		pre := other.(*Foo)

		if pre.Name != post.Name {
			return []Action{
				Action{
					"RENAME",
					"OBJ",
					post,
				},
			}
		}
	}

	return []Action{}
}

func (post *Bar) Diff(other Diffable) []Action {
	if other == nil {
		return []Action{
			Action{
				"CREATE",
				"OBJ",
				post,
			},
		}
	} else {
		pre := other.(*Bar)

		if pre.Name != post.Name {
			return []Action{
				Action{
					"RENAME",
					"OBJ",
					post,
				},
			}
		}
	}

	return []Action{}
}

func (f *Foo) Children() []Diffable {
	children := make([]Diffable, 0)

	for i, _ := range f.Bars {
		children = append(children, f.Bars[i])
	}

	return children
}

func (b *Bar) Children() []Diffable {
	return []Diffable{}
}

func (f *Foo) Drop() []Action {
	return []Action{
		Action{
			"DROP",
			"OBJ",
			f,
		},
	}
}

func (b *Bar) Drop() []Action {
	return []Action{
		Action{
			"DROP",
			"OBJ",
			b,
		},
	}
}

func (f *Foo) IsEqual(other Diffable) bool {
	if other == nil {
		return false
	}

	otherFoo := other.(*Foo)
	return (otherFoo.Id == f.Id)
}

func (b *Bar) IsEqual(other Diffable) bool {
	if other == nil {
		return false
	}

	otherBar := other.(*Bar)
	return (otherBar.Id == b.Id)
}

// Test a diff that should create something
func TestDiffCreate(t *testing.T) {
	pre := []Diffable{}

	post := []Diffable{
		Diffable(NewFoo("test", 1, []*Bar{})),
	}

	actions := Diff(pre, post)

	if len(actions) != 1 {
		t.Errorf("len actions => %d, want %d", len(actions), 1)
	}

	if actions[0].Kind != "CREATE" {
		t.Errorf("action kind => %s, want %s", actions[0].Kind, "CREATE")
	}

	if actions[0].Object != post[0] {
		t.Errorf("action object => %v, want %v", actions[0].Object, post[0])
	}
}

// Test a diff that should rename something
func TestDiffRename(t *testing.T) {
	pre := []Diffable{
		Diffable(NewFoo("test", 1, []*Bar{})),
	}

	post := []Diffable{
		Diffable(NewFoo("testing this", 1, []*Bar{})),
	}

	actions := Diff(pre, post)

	if len(actions) != 1 {
		t.Errorf("len actions => %d, want %d", len(actions), 1)
	}

	if actions[0].Kind != "RENAME" {
		t.Errorf("action kind => %s, want %s", actions[0].Kind, "RENAME")
	}

	if actions[0].Object != post[0] {
		t.Errorf("action object => %v, want %v", actions[0].Object, post[0])
	}
}

// Test a diff that should drop something
func TestDiffDrop(t *testing.T) {
	pre := []Diffable{
		Diffable(NewFoo("test", 1, []*Bar{})),
	}

	post := []Diffable{}

	actions := Diff(pre, post)

	if len(actions) != 1 {
		t.Errorf("len actions => %d, want %d", len(actions), 1)
	}

	if actions[0].Kind != "DROP" {
		t.Errorf("action kind => %s, want %s", actions[0].Kind, "RENAME")
	}

	if actions[0].Object != pre[0] {
		t.Errorf("action object => %v, want %v", actions[0].Object, pre[0])
	}
}

// Test a diff that should create something recursively
func TestDiffCreateRecursively(t *testing.T) {
	pre := []Diffable{
		Diffable(NewFoo("test", 1, []*Bar{})),
	}

	post := []Diffable{
		Diffable(NewFoo("test", 1, []*Bar{NewBar("sub test", 1)})),
	}

	actions := Diff(pre, post)

	if len(actions) != 1 {
		t.Errorf("len actions => %d, want %d", len(actions), 1)
	}

	if actions[0].Kind != "CREATE" {
		t.Errorf("action kind => %s, want %s", actions[0].Kind, "CREATE")
	}

	foo := post[0].(*Foo)

	if actions[0].Object != foo.Bars[0] {
		t.Errorf("action object => %v, want %v", actions[0].Object, foo.Bars[0])
	}
}

// Test a diff that should rename something recursively
func TestDiffRenameRecursively(t *testing.T) {
	pre := []Diffable{
		Diffable(NewFoo("test", 1, []*Bar{NewBar("sub test", 1)})),
	}

	post := []Diffable{
		Diffable(NewFoo("test", 1, []*Bar{NewBar("sub test edited", 1)})),
	}

	actions := Diff(pre, post)

	if len(actions) != 1 {
		t.Errorf("len actions => %d, want %d", len(actions), 1)
	}

	if actions[0].Kind != "RENAME" {
		t.Errorf("action kind => %s, want %s", actions[0].Kind, "RENAME")
	}

	foo := post[0].(*Foo)

	if actions[0].Object != foo.Bars[0] {
		t.Errorf("action object => %v, want %v", actions[0].Object, foo.Bars[0])
	}
}

// Test a diff that should drop something recursively
func TestDiffDropRecursively(t *testing.T) {
	pre := []Diffable{
		Diffable(NewFoo("test", 1, []*Bar{NewBar("sub test", 1)})),
	}

	post := []Diffable{
		Diffable(NewFoo("test", 1, []*Bar{})),
	}

	actions := Diff(pre, post)

	if len(actions) != 1 {
		t.Errorf("len actions => %d, want %d", len(actions), 1)
	}

	if actions[0].Kind != "DROP" {
		t.Errorf("action kind => %s, want %s", actions[0].Kind, "DROP")
	}

	foo := pre[0].(*Foo)

	if actions[0].Object != foo.Bars[0] {
		t.Errorf("action object => %v, want %v", actions[0].Object, foo.Bars[0])
	}
}
