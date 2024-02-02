package builder

import (
	"testing"
	"time"
)

func TestBuildInStatement(t *testing.T) {
	type test struct {
		AB string `json:"ab" db:"a_b"`
		CD string `json:"cd" db:"c_d"`
	}
	result, values := buildInStatement(&test{}, "AB", []string{"satu", "dua"})
	val0 := values[0].(string)
	val1 := values[1].(string)
	if val0 != "satu" || val1 != "dua" {
		t.Fatal("comparator statement return wrong value")
	}
	if result != "`AB` IN (?,?) " {
		t.Fatal("wrong statement")
	}
}

func TestQuery(t *testing.T) {

	//create in statement
	in := make(map[string]interface{})
	in["Hello"] = []string{"one", "two", "three"}

	//create comparator
	comparators := []Condition{{
		Operator: "like",
		Key:      "hello(`satu`)",
		Value:    "dua",
	}}

	query, values := New().
		Select("test").
		From("`hello_world`").
		Statement("", make([]interface{}, 0)).
		And().
		In(in).
		And().
		Compare(comparators).
		And().
		BetweenTime(`record_time`, time.Now(), time.Now().Add(24*time.Hour)).
		Page(2).Size(20).Build()

	t.Log(query)

	if len(query) == 0 {
		t.Fatal("query is empty")
	}
	if values[0] != "one" {
		t.Fatal("values is empty")
	}
}

func TestJoin(t *testing.T) {
	//create in statement
	in := make(map[string]interface{})
	in["Hello"] = []string{"one", "two", "three"}

	//create comparator
	query, _ := New().
		Select("test").
		From("`hello_world`", "a").
		Join("`bye_world`", "a.why = b.why", "b").Build()
	if len(query) == 0 {
		t.Fatal("query is empty")
	}
	t.Log(query)
}

func TestUpsert(t *testing.T) {
	query, _ := New().From("`hello_world`", "a").Upsert(map[string]interface{}{
		"`value`": "name",
		"`key`":   "`key` + 1.2 ",
	}).Build()
	if len(query) == 0 {
		t.Fatal("query is empty")
	}
	t.Log(query)
	query, _ = New().From("`hello_world`", "a").Upsert(map[string]interface{}{
		"`value`": "name",
		"`key`":   "lower(`key`)",
	}).Build()
	if len(query) == 0 {
		t.Fatal("query is empty")
	}
	t.Log(query)
}
