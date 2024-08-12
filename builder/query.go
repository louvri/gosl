package builder

import (
	"fmt"
	"strings"
	"time"
)

const DateTimeFormat = "2006-01-02 15:04:05"

type Builder interface {
	Insert(data map[string]interface{}, columns ...string) Builder
	Update(data map[string]interface{}, columns ...string) Builder
	Upsert(data map[string]interface{}, columns ...string) Builder
	Delete() Builder
	Explain() Builder
	Select(field string) Builder
	Table(table string, alias ...string) Builder
	From(table string, alias ...string) Builder
	Join(table string, on string, alias ...string) Builder
	LeftJoin(table string, on string, alias ...string) Builder
	RightJoin(table string, on string, alias ...string) Builder
	Statement(stmt string, values []interface{}) Builder
	In(in map[string]interface{}) Builder
	Exists(other Builder, condition Condition) Builder
	Alias(name string) string
	Compare(conditions []Condition) Builder
	NotEqual(column string, value interface{}) Builder
	Equal(column string, value interface{}) Builder
	BetweenTime(column string, from, to time.Time) Builder
	Page(index int) Builder
	Size(n int) Builder
	Order(order OrderBy) Builder
	Orders(orders []OrderBy) Builder
	Group(column string) Builder
	Groups(column []string) Builder
	And() Builder
	Or() Builder
	Not(Builder) Builder
	Status() (int, int, int)
	Reset(section string) Builder
	Build() (string, []interface{})
}

func New() Builder {
	return &builder{}
}

type builder struct {
	operator        []string
	source          []map[string]string
	selectStatement strings.Builder
	whereStatement  strings.Builder
	orderStatement  strings.Builder
	groupStatement  strings.Builder
	values          []interface{}
	page            int
	size            int
	explain         bool
	upsert          map[string]interface{}
	update          map[string]interface{}
	insert          map[string]interface{}
	delete          bool
	columns         []string
}

func (b *builder) Insert(data map[string]interface{}, columns ...string) Builder {
	b.insert = data
	b.columns = columns
	return b
}
func (b *builder) Update(data map[string]interface{}, columns ...string) Builder {
	b.update = data
	b.columns = columns
	return b
}
func (b *builder) Upsert(data map[string]interface{}, columns ...string) Builder {
	b.upsert = data
	b.columns = columns
	return b
}
func (b *builder) Delete() Builder {
	b.delete = true
	return b
}
func (b *builder) Explain() Builder {
	b.explain = true
	return b
}
func (b *builder) Select(field string) Builder {
	if b.selectStatement.Len() > 0 {
		b.selectStatement.WriteString(",")
	}
	b.selectStatement.WriteString(field)
	return b
}

func (b *builder) Table(table string, alias ...string) Builder {
	tmp := make(map[string]string)
	tmp["table"] = table
	tmp["operator"] = "FROM"
	if len(alias) > 0 {
		tmp["alias"] = alias[0]
	}
	b.source = append(b.source, tmp)
	return b
}
func (b *builder) From(table string, alias ...string) Builder {
	tmp := make(map[string]string)
	tmp["table"] = table
	tmp["operator"] = "FROM"
	if len(alias) > 0 {
		tmp["alias"] = alias[0]
	}
	b.source = append(b.source, tmp)
	return b
}
func (b *builder) Join(table string, on string, alias ...string) Builder {
	tmp := make(map[string]string)
	tmp["table"] = table
	tmp["on"] = on
	tmp["operator"] = "JOIN"
	if len(alias) > 0 {
		tmp["alias"] = alias[0]
	}
	b.source = append(b.source, tmp)
	return b
}
func (b *builder) LeftJoin(table string, on string, alias ...string) Builder {
	tmp := make(map[string]string)
	tmp["table"] = table
	tmp["on"] = on
	tmp["operator"] = "LEFT JOIN"
	if len(alias) > 0 {
		tmp["alias"] = alias[0]
	}
	b.source = append(b.source, tmp)
	return b
}
func (b *builder) RightJoin(table string, on string, alias ...string) Builder {
	tmp := make(map[string]string)
	tmp["table"] = table
	tmp["on"] = on
	tmp["operator"] = "RIGHT JOIN"
	if len(alias) > 0 {
		tmp["alias"] = alias[0]
	}
	b.source = append(b.source, tmp)
	return b
}
func (b *builder) Alias(name string) string {
	for _, item := range b.source {
		if item["table"] == name {
			if item["alias"] == "" {
				return item["table"]
			}
			return item["alias"]
		}
	}
	return ""
}

func (b *builder) Statement(stmt string, values []interface{}) Builder {
	if b.whereStatement.Len() > 0 {
		b.whereStatement.WriteString(b.operator[0])
		b.operator = b.operator[1:]
	}
	b.whereStatement.WriteString(stmt)
	b.values = append(b.values, values...)
	return b
}

func (b *builder) In(in map[string]interface{}) Builder {
	if b.whereStatement.Len() > 0 {
		b.whereStatement.WriteString(b.operator[0])
		b.operator = b.operator[1:]
	}
	first := true
	for key, value := range in {
		if query, values := buildInStatement(key, value); len(query) > 0 {
			if !first {
				b.whereStatement.WriteString(" AND ")
			}
			b.whereStatement.WriteString(query)
			b.values = append(b.values, values...)
			first = false
		}
	}
	return b
}

func (b *builder) Exists(other Builder, condition Condition) Builder {
	if b.whereStatement.Len() > 0 {
		b.whereStatement.WriteString(b.operator[0])
		b.operator = b.operator[1:]
	}
	_other := other.(*builder)
	var tmp strings.Builder
	conditionStatement, _ := buildConditionStatement(condition)
	tmp.WriteString(conditionStatement)
	if _other.whereStatement.Len() > 0 {
		tmp.WriteString(" AND ")
	}
	tmp.WriteString(_other.whereStatement.String())
	_other.whereStatement = tmp
	b.whereStatement.WriteString(" EXISTS (")
	innerStatement, values := _other.Build()
	b.whereStatement.WriteString(innerStatement)
	b.whereStatement.WriteString(")")
	b.values = append(b.values, values...)
	return b
}

func (b *builder) Compare(conditions []Condition) Builder {
	var compStatement strings.Builder
	for _, item := range conditions {
		stmt, value := buildConditionStatement(item)
		if len(stmt) > 0 {
			if compStatement.Len() > 0 {
				compStatement.WriteString(" AND ")
			}
			compStatement.WriteString(stmt)
		}
		if value != nil {
			b.values = append(b.values, value)
		}
	}
	if compStatement.Len() > 0 {
		if b.whereStatement.Len() > 0 {
			b.whereStatement.WriteString(b.operator[0])
			b.operator = b.operator[1:]
		}
		b.whereStatement.WriteString(compStatement.String())
	}
	return b
}

func (b *builder) NotEqual(column string, value interface{}) Builder {
	if !strings.Contains(column, "`") {
		column = "`" + column + "`"
	}
	if b.whereStatement.Len() > 0 {
		b.whereStatement.WriteString(b.operator[0])
		b.operator = b.operator[1:]
	}
	b.whereStatement.WriteString(column)
	b.whereStatement.WriteString(" <> ")
	b.whereStatement.WriteString("?")
	b.values = append(b.values, value)
	return b
}
func (b *builder) Equal(column string, value interface{}) Builder {
	if !strings.Contains(column, "`") {
		column = "`" + column + "`"
	}
	if b.whereStatement.Len() > 0 {
		b.whereStatement.WriteString(b.operator[0])
		b.operator = b.operator[1:]
	}
	b.whereStatement.WriteString(column)
	b.whereStatement.WriteString(" = ")
	b.whereStatement.WriteString("?")
	b.values = append(b.values, value)
	return b
}
func (b *builder) BetweenTime(column string, from, to time.Time) Builder {
	if !strings.Contains(column, "`") {
		column = "`" + column + "`"
	}
	if b.whereStatement.Len() > 0 {
		b.whereStatement.WriteString(b.operator[0])
		b.operator = b.operator[1:]
	}
	b.whereStatement.WriteString(column)
	b.whereStatement.WriteString(" BETWEEN ? AND ? ")
	b.values = append(b.values, from.Format(DateTimeFormat))
	b.values = append(b.values, to.Format(DateTimeFormat))
	return b
}
func (b *builder) Page(index int) Builder {
	if index > 0 {
		b.page = index - 1
	}

	return b
}
func (b *builder) Size(n int) Builder {
	if n > 0 {
		b.size = n
	}
	return b
}
func (b *builder) Order(order OrderBy) Builder {
	if !strings.Contains(order.Column, "`") {
		order.Column = "`" + order.Column + "`"
	}
	if b.orderStatement.Len() > 0 {
		b.orderStatement.WriteString(",")
	}
	if len(order.Fields) > 0 {
		b.orderStatement.WriteString("Field(")
		b.orderStatement.WriteString(order.Column)
		b.orderStatement.WriteString(",")
		for i, field := range order.Fields {
			if i > 0 {
				b.orderStatement.WriteString(",")
			}
			b.orderStatement.WriteString("'")
			b.orderStatement.WriteString(field)
			b.orderStatement.WriteString("'")
		}
		b.orderStatement.WriteString(")")
	} else {
		b.orderStatement.WriteString(order.Column)
		b.orderStatement.WriteString(" ")
		b.orderStatement.WriteString(order.Direction)
	}
	return b
}
func (b *builder) Orders(orders []OrderBy) Builder {
	for _, order := range orders {
		b.Order(order)
	}
	return b
}
func (b *builder) Group(column string) Builder {
	if !strings.Contains(column, "`") {
		column = "`" + column + "`"
	}
	if b.groupStatement.Len() > 0 {
		b.groupStatement.WriteString(",")
	}
	b.groupStatement.WriteString(column)
	b.groupStatement.WriteString(" ")
	return b
}
func (b *builder) Groups(columns []string) Builder {
	for _, column := range columns {
		if !strings.Contains(column, "`") {
			column = "`" + column + "`"
		}
		if b.groupStatement.Len() > 0 {
			b.groupStatement.WriteString(",")
		}
		b.groupStatement.WriteString(column)
		b.groupStatement.WriteString(" ")
	}

	return b
}
func (b *builder) And() Builder {
	b.operator = append(b.operator, " AND ")
	return b
}
func (b *builder) Or() Builder {
	b.operator = append(b.operator, " OR ")
	return b
}
func (b *builder) Not(inner Builder) Builder {
	if b.whereStatement.Len() > 0 {
		b.whereStatement.WriteString(b.operator[0])
		b.operator = b.operator[1:]
	}
	_inner := inner.(*builder)
	b.whereStatement.WriteString("not(")
	b.whereStatement.WriteString(_inner.whereStatement.String())
	b.whereStatement.WriteString(")")
	b.values = append(b.values, _inner.values...)
	return b
}
func (b *builder) Status() (int, int, int) {
	return b.selectStatement.Len(), b.whereStatement.Len(), b.orderStatement.Len()
}
func (b *builder) Reset(section string) Builder {
	switch section {
	case "select", "Select", "SELECT":
		b.selectStatement.Reset()
	case "from":
		b.source = make([]map[string]string, 0)
	case "where", "Where", "WHERE":
		b.whereStatement.Reset()
	case "orderby", "Orderby", "ORDERBY":
		b.orderStatement.Reset()
	case "table", "Table", "TABLE":
		b.source = nil
	}
	return b
}
func (b *builder) Build() (string, []interface{}) {
	var values []interface{}
	var query strings.Builder
	if len(b.insert) > 0 {
		var stmt string
		stmt, values = buildInsert(b.source[0]["table"], b.insert, b.columns)
		query.WriteString(stmt)
	} else if len(b.upsert) > 0 {
		var stmt string
		stmt, values = buildUpsert(b.source[0]["table"], b.upsert, b.columns)
		query.WriteString(stmt)
	} else if len(b.update) > 0 {
		var stmt string
		stmt, values = buildUpdate(b.source[0]["table"], b.update, b.columns)
		query.WriteString(stmt)
		if b.whereStatement.Len() > 0 {
			query.WriteString(" ")
			query.WriteString("WHERE ")
			query.WriteString(b.whereStatement.String())
			values = append(values, b.values...)
		}
	} else if b.delete {
		query.WriteString("DELETE ")
		query.WriteString("FROM ")
		query.WriteString(b.source[0]["table"])
		if b.whereStatement.Len() > 0 {
			query.WriteString("WHERE ")
			query.WriteString(b.whereStatement.String())
		}
		values = b.values
	} else {
		if b.explain {
			query.WriteString("EXPLAIN ")
		}
		query.WriteString("SELECT ")
		query.WriteString(b.selectStatement.String())
		query.WriteString(" ")
		query.WriteString("FROM ")
		query.WriteString(b.source[0]["table"])
		if b.source[0]["alias"] != "" {
			query.WriteString(" ")
			query.WriteString(b.source[0]["alias"])
		}
		for _, src := range b.source[1:] {
			query.WriteString(" ")
			query.WriteString(src["operator"])
			query.WriteString(" ")
			query.WriteString(src["table"])
			if src["alias"] != "" {
				query.WriteString(" ")
				query.WriteString(src["alias"])
			}
			query.WriteString(" ")
			query.WriteString(" ON ")
			query.WriteString(src["on"])
		}
		if b.whereStatement.Len() > 0 {
			query.WriteString(" ")
			query.WriteString("WHERE ")
			query.WriteString(b.whereStatement.String())
		}
		if b.groupStatement.Len() > 0 {
			query.WriteString(" ")
			query.WriteString("GROUP BY ")
			query.WriteString(b.groupStatement.String())
		}
		if b.orderStatement.Len() > 0 {
			query.WriteString(" ")
			query.WriteString("ORDER BY ")
			query.WriteString(b.orderStatement.String())
		}
		if b.size != 0 {
			query.WriteString(" ")
			query.WriteString(fmt.Sprintf("LIMIT %d ", b.size))
		}
		if b.page != 0 {
			query.WriteString(" ")
			query.WriteString(fmt.Sprintf("OFFSET %d ", b.page*b.size))
		}
		values = b.values
	}

	return query.String(), values
}
