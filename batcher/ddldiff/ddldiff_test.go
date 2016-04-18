package ddldiff

import (
	"github.com/pagarme/teleport/action"
	"testing"
)

// Foo implements Diffable
type Foo struct {
	Name string
	Id   int
	Bars []*Bar
}

type Bar struct {
	Name string
	Id   int
}

type FooAction struct {
	Kind string
}

func (a *FooAction) Execute(c action.Context) error {
	return nil
}

func (a *FooAction) Filter(targetExpression string) bool {
	return true
}

func (a *FooAction) NeedsSeparatedBatch() bool {
	return false
}

type BarAction struct {
	Kind string
}

func (a *BarAction) Execute(c action.Context) error {
	return nil
}

func (a *BarAction) Filter(targetExpression string) bool {
	return true
}

func (a *BarAction) NeedsSeparatedBatch() bool {
	return false
}

func NewFoo(name string, id int, bars []*Bar) *Foo {
	return &Foo{name, id, bars}
}

func NewBar(name string, id int) *Bar {
	return &Bar{name, id}
}

func (post *Foo) Diff(other Diffable) []action.Action {
	if other == nil {
		return []action.Action{
			&FooAction{
				"CREATE FOO",
			},
		}
	} else {
		pre := other.(*Foo)

		if pre.Name != post.Name {
			return []action.Action{
				&FooAction{
					"RENAME FOO",
				},
			}
		}
	}

	return []action.Action{}
}

func (post *Bar) Diff(other Diffable) []action.Action {
	if other == nil {
		return []action.Action{
			&BarAction{
				"CREATE BAR",
			},
		}
	} else {
		pre := other.(*Bar)

		if pre.Name != post.Name {
			return []action.Action{
				&BarAction{
					"RENAME BAR",
				},
			}
		}
	}

	return []action.Action{}
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

func (f *Foo) Drop() []action.Action {
	return []action.Action{
		&FooAction{
			"DROP FOO",
		},
	}
}

func (b *Bar) Drop() []action.Action {
	return []action.Action{
		&BarAction{
			"DROP BAR",
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

// // Test a diff that should create something
func TestDiffCreate(t *testing.T) {
	pre := []Diffable{}

	post := []Diffable{
		Diffable(NewFoo("test", 1, []*Bar{})),
	}

	actions := Diff(pre, post)

	if len(actions) != 1 {
		t.Errorf("len actions => %d, want %d", len(actions), 1)
	}

	fooAction := actions[0].(*FooAction)

	if fooAction.Kind != "CREATE FOO" {
		t.Errorf("action kind => %s, want %s", fooAction.Kind, "CREATE FOO")
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

	fooAction := actions[0].(*FooAction)

	if fooAction.Kind != "RENAME FOO" {
		t.Errorf("action kind => %s, want %s", fooAction.Kind, "RENAME FOO")
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

	fooAction := actions[0].(*FooAction)

	if fooAction.Kind != "DROP FOO" {
		t.Errorf("action kind => %s, want %s", fooAction.Kind, "DROP FOO")
	}
}

// Test a diff that should create something recursively
func TestDiffCreateTree(t *testing.T) {
	pre := []Diffable{}

	post := []Diffable{
		Diffable(NewFoo("test", 1, []*Bar{NewBar("sub test", 1)})),
	}

	actions := Diff(pre, post)

	if len(actions) != 2 {
		t.Errorf("len actions => %d, want %d", len(actions), 2)
	}

	fooAction := actions[0].(*FooAction)
	barAction := actions[1].(*BarAction)

	if fooAction.Kind != "CREATE FOO" {
		t.Errorf("action kind => %s, want %s", fooAction.Kind, "CREATE FOO")
	}

	if barAction.Kind != "CREATE BAR" {
		t.Errorf("action kind => %s, want %s", barAction.Kind, "CREATE BAR")
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

	barAction := actions[0].(*BarAction)

	if barAction.Kind != "CREATE BAR" {
		t.Errorf("action kind => %s, want %s", barAction.Kind, "CREATE BAR")
	}
}

// Test a diff that should rename something recursively
func TestDiffRenameRecursively(t *testing.T) {
	pre := []Diffable{
		Diffable(NewFoo("test", 1, []*Bar{NewBar("sub test", 1)})),
	}

	post := []Diffable{
		Diffable(NewFoo("test edited", 1, []*Bar{NewBar("sub test edited", 1)})),
	}

	actions := Diff(pre, post)

	if len(actions) != 2 {
		t.Errorf("len actions => %d, want %d", len(actions), 2)
	}

	fooAction := actions[0].(*FooAction)
	barAction := actions[1].(*BarAction)

	if fooAction.Kind != "RENAME FOO" {
		t.Errorf("action kind => %s, want %s", fooAction.Kind, "RENAME FOO")
	}

	if barAction.Kind != "RENAME BAR" {
		t.Errorf("action kind => %s, want %s", barAction.Kind, "RENAME BAR")
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

	barAction := actions[0].(*BarAction)

	if barAction.Kind != "DROP BAR" {
		t.Errorf("action kind => %s, want %s", barAction.Kind, "DROP BAR")
	}
}
