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
		"`key`":   Expr("`key` + 1.2 "),
	}).Build()
	if len(query) == 0 {
		t.Fatal("query is empty")
	}
	t.Log(query, values)
	query, values = New().From("`hello_world`", "a").Upsert(map[string]interface{}{
		"`value`": "name",
		"`key`":   Expr("lower(`key`)"),
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

func TestPlainStringWithBacktickIsBound(t *testing.T) {
	address := "Jl. D`Arcy 5"

	query, values := New().From("`vip_order`").Update(map[string]interface{}{
		"recipient_address": address,
	}).Build()
	if strings.Contains(query, address) || len(values) != 1 || values[0] != address {
		t.Fatalf("update wrote value unbound: %s %v", query, values)
	}

	query, values = New().From("`vip_order`").Upsert(map[string]interface{}{
		"recipient_address": address,
	}).Build()
	if strings.Contains(query, address) || len(values) != 2 || values[0] != address || values[1] != address {
		t.Fatalf("upsert wrote value unbound: %s %v", query, values)
	}

	stmt, value := buildConditionStatement(Condition{Operator: "=", Key: "recipient_address", Value: address})
	if strings.Contains(stmt, address) || value != address {
		t.Fatalf("condition wrote value unbound: %s %v", stmt, value)
	}
}

func TestExprIsWrittenRaw(t *testing.T) {
	query, values := New().From("`stock`").Update(map[string]interface{}{
		"qty": Expr("`qty` - 1"),
	}).Build()
	if !strings.Contains(query, "`qty`=`qty` - 1") || len(values) != 0 {
		t.Fatalf("update did not write expr: %s %v", query, values)
	}

	stmt, value := buildConditionStatement(Condition{Operator: "=", Key: "a", Value: Expr("lower(`b`)")})
	if !strings.HasSuffix(stmt, "lower(`b`)") || value != nil {
		t.Fatalf("condition did not write expr: %s %v", stmt, value)
	}
}

func TestOrderOnlyWritesIdentifiersAndDirection(t *testing.T) {
	query, values := New().Select("*").From("t").Equal("a", 1).Orders([]OrderBy{
		{Column: "id", Direction: "asc,(select sleep(5))"},
		{Column: "x(select 1).id", Direction: "desc"},
		{Column: "`id` desc,(select sleep(5))"},
		{Column: "b.`record_time`", Direction: " DESC "},
		{Column: "waybill_no", Fields: []string{"A", "B') or sleep(5) -- "}},
	}).Build()
	want := "ORDER BY `id` ASC,`b`.`record_time` DESC,Field(`waybill_no`,?,?)"
	if !strings.Contains(query, want) || strings.Contains(query, "sleep") {
		t.Fatalf("unexpected order by: %s", query)
	}
	if len(values) != 3 || values[0] != 1 || values[1] != "A" || values[2] != "B') or sleep(5) -- " {
		t.Fatalf("unexpected values: %v", values)
	}
}

func TestExistsBindsConditionValueFirst(t *testing.T) {
	inner := New().Select("1").From("`orders`", "o").Equal("status", "paid")
	query, values := New().Select("*").From("`users`", "u").Equal("active", 1).And().
		Exists(inner, Condition{Key: "o.user_id", Operator: "=", Value: 5}).Build()
	if !strings.Contains(query, "EXISTS (SELECT 1 FROM `orders` o WHERE o.`user_id`= ? AND ") {
		t.Fatalf("unexpected exists: %s", query)
	}
	if len(values) != 3 || values[0] != 1 || values[1] != 5 || values[2] != "paid" {
		t.Fatalf("unexpected values: %v", values)
	}

	inner = New().Select("1").From("`orders`", "o")
	query, values = New().Select("*").From("`users`", "u").
		Exists(inner, Condition{Key: "o.user_id", Operator: "=", Value: Expr("`u`.`id`")}).Build()
	if !strings.Contains(query, "o.`user_id`= `u`.`id`") || len(values) != 0 {
		t.Fatalf("exists did not write expr: %s %v", query, values)
	}
}

func TestUpsertWritesExprRawOnBothSides(t *testing.T) {
	query, values := New().From("`stock`").Upsert(map[string]interface{}{
		"qty": Expr("`qty` - 1"),
	}).Build()
	want := "INSERT INTO `stock`(`qty`) VALUES (`qty` - 1) ON DUPLICATE KEY UPDATE `qty`=`qty` - 1;"
	if query != want || len(values) != 0 {
		t.Fatalf("got %s %v, want %s", query, values, want)
	}
}

func TestColumnsAreSortedWhenNoneGiven(t *testing.T) {
	data := map[string]interface{}{
		"total": Expr("`price` * 2"),
		"price": 10,
		"name":  "a",
	}
	for i := 0; i < 20; i++ {
		query, values := New().From("`item`").Upsert(data).Build()
		want := "INSERT INTO `item`(`name`,`price`,`total`) VALUES (?,?,`price` * 2) ON DUPLICATE KEY UPDATE `name`=?,`price`=?,`total`=`price` * 2;"
		if query != want || len(values) != 4 || values[0] != "a" || values[1] != 10 {
			t.Fatalf("got %s %v, want %s", query, values, want)
		}
		query, _ = New().From("`item`").Update(data).Build()
		if query != "UPDATE `item` SET `name`=?,`price`=?,`total`=`price` * 2" {
			t.Fatalf("unexpected update: %s", query)
		}
		query, _ = New().From("`item`").Insert(map[string]interface{}{"b": 1, "a": 2}).Build()
		if query != "INSERT INTO `item`(`a`,`b`) VALUES (?,?);" {
			t.Fatalf("unexpected insert: %s", query)
		}
	}
}
