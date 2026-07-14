package builder

import (
	"strings"
	"testing"
	"time"
)

func TestBuildInStatement(t *testing.T) {
	result, values := buildInStatement("AB", []string{"satu", "dua"})
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
	query, values := New().From("`hello_world`", "a").Upsert(map[string]interface{}{
		"`value`": "name",
		"`key`":   Raw("`key` + 1.2 "),
	}).Build()
	if len(query) == 0 {
		t.Fatal("query is empty")
	}
	t.Log(query, values)
	query, values = New().From("`hello_world`", "a").Upsert(map[string]interface{}{
		"`value`": "name",
		"`key`":   Raw("lower(`key`)"),
	}).Build()
	t.Log(query, values)
	if len(query) == 0 {
		t.Fatal("query is empty")
	}
	query, values = New().From("`hello_world`", "b").Upsert(map[string]interface{}{
		"value": nil,
		"`key`": "baru",
	}).Build()
	t.Log(query, values)
	if len(query) == 0 {
		t.Fatal("query is empty")
	}
}

// TestUpsertBacktickInValueIsBound guards against a regression where a data
// value containing a literal backtick was interpolated verbatim into the SQL
// statement instead of being bound as a parameter. That behavior produced the
// MySQL 1064 syntax error observed in production and was also a SQL injection
// vector: any user-supplied string containing "`" could inject SQL.
func TestUpsertBacktickInValueIsBound(t *testing.T) {
	payload := "QA Courier C`manggis "
	query, values := New().From("`mps_item_tracker`").Upsert(map[string]interface{}{
		"`operation_user_name`": payload,
		"`order_no`":            "N1",
	}).Build()

	if strings.Contains(query, payload) {
		t.Fatalf("value with backtick was interpolated into SQL: %q", query)
	}
	found := false
	for _, v := range values {
		if s, ok := v.(string); ok && s == payload {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("value with backtick was not bound as parameter: values=%v", values)
	}
}

func TestUpdateBacktickInValueIsBound(t *testing.T) {
	payload := "raw`data"
	query, values := New().From("`t`").Update(map[string]interface{}{
		"`name`": payload,
	}).Equal("id", 1).Build()

	if strings.Contains(query, payload) {
		t.Fatalf("value with backtick was interpolated into SQL: %q", query)
	}
	found := false
	for _, v := range values {
		if s, ok := v.(string); ok && s == payload {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("value with backtick was not bound as parameter: values=%v", values)
	}
}

func TestCompareBacktickInValueIsBound(t *testing.T) {
	payload := "attacker`;--"
	query, values := New().Select("*").From("`t`").Compare([]Condition{{
		Key:      "name",
		Operator: "=",
		Value:    payload,
	}}).Build()

	if strings.Contains(query, payload) {
		t.Fatalf("condition value with backtick was interpolated into SQL: %q", query)
	}
	found := false
	for _, v := range values {
		if s, ok := v.(string); ok && s == payload {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("condition value with backtick was not bound as parameter: values=%v", values)
	}
}

func TestUpsertRawIsInlined(t *testing.T) {
	query, values := New().From("`counter_t`").Upsert(map[string]interface{}{
		"`counter`": Raw("`counter` + 1"),
		"`name`":    "widget",
	}).Build()

	if !strings.Contains(query, "`counter` + 1") {
		t.Fatalf("Raw fragment was not inlined: %q", query)
	}
	for _, v := range values {
		if s, ok := v.(string); ok && strings.Contains(s, "`counter`") {
			t.Fatalf("Raw fragment was bound as parameter instead of inlined: %v", values)
		}
	}
}
