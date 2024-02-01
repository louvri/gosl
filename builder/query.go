package builder

import (
	"fmt"
	"regexp"
	"strings"
	"time"
)

const DateTimeFormat = "2006-01-02 15:04:05"

type Builder interface {
	Insert(data map[string]interface{}) Builder
	Update(data map[string]interface{}) Builder
	Upsert(data map[string]interface{}) Builder
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
	InSingleProp(prop string, data interface{}) Builder
	Exists(other Builder, condition Condition) Builder
	Alias(name string) string
	Compare(conditions []Condition) Builder
	CompareSingleProp(condition Condition) Builder
	NotEqual(column string, value interface{}) Builder
	Equal(column string, value interface{}) Builder
	BetweenTime(column string, from, to time.Time) Builder
	Next(id int64) Builder
	Page(index int) Builder
	Size(n int) Builder
	Order(column, direction string) Builder
	Orders(orders map[string]interface{}) Builder
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
	return &builder{
		orderMap: make(map[string]string),
	}
}

type builder struct {
	ref             interface{}
	operator        []string
	source          []map[string]string
	selectStatement strings.Builder
	whereStatement  strings.Builder
	orderStatement  strings.Builder
	groupStatement  strings.Builder
	orderMap        map[string]string
	values          []interface{}
	next            int64
	page            int
	size            int
	explain         bool
	upsert          map[string]interface{}
	update          map[string]interface{}
	insert          map[string]interface{}
	delete          bool
}

func (b *builder) Insert(data map[string]interface{}) Builder {
	b.insert = data
	return b
}
func (b *builder) Update(data map[string]interface{}) Builder {
	b.update = data
	return b
}
func (b *builder) Upsert(data map[string]interface{}) Builder {
	b.upsert = data
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
		if query, values := buildInStatement(b.ref, key, value); len(query) > 0 {
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

func (b *builder) InSingleProp(prop string, data interface{}) Builder {
	if b.whereStatement.Len() > 0 {
		b.whereStatement.WriteString(b.operator[0])
		b.operator = b.operator[1:]
	}
	if query, values := buildInStatement(b.ref, prop, data); len(query) > 0 {
		b.whereStatement.WriteString(query)
		b.values = append(b.values, values...)
	}
	return b
}

func (b *builder) Exists(other Builder, condition Condition) Builder {
	if b.whereStatement.Len() > 0 {
		b.whereStatement.WriteString(b.operator[0])
		b.operator = b.operator[1:]
	}
	_other := other.(*builder)
	if _other.whereStatement.Len() > 0 {
		_other.whereStatement.WriteString(" AND ")
	}
	conditionStatement, _ := buildConditionStatement(condition)
	_other.whereStatement.WriteString(conditionStatement)

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

func (b *builder) CompareSingleProp(condition Condition) Builder {
	var compStatement strings.Builder
	stmt, value := buildConditionStatement(condition)
	if len(stmt) > 0 {
		compStatement.WriteString(stmt)
	}
	if value != nil {
		b.values = append(b.values, value)
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
func (b *builder) Next(id int64) Builder {
	b.next = id
	return b
}
func (b *builder) Page(index int) Builder {
	b.page = index - 1
	return b
}
func (b *builder) Size(n int) Builder {
	b.size = n
	return b
}
func (b *builder) Order(column, direction string) Builder {
	if b.orderStatement.Len() > 0 {
		b.orderStatement.WriteString(",")
	}
	b.orderStatement.WriteString(column)
	b.orderStatement.WriteString(" ")
	b.orderStatement.WriteString(direction)
	return b
}
func (b *builder) Orders(orders map[string]interface{}) Builder {
	priorityOrdersMap := make(map[int][]string)
	var orderByStatement strings.Builder
	var direction string
	var priority int
	idx := 1
	for column, rule := range orders {
		if tmp, ok := rule.(map[string]interface{}); ok {
			priority = tmp["priority"].(int)
			direction = tmp["direction"].(string)
		} else {
			priority = idx
			direction = rule.(string)
		}
		priorityOrdersMap[priority] = []string{column, direction}
		b.orderMap[column] = direction
		idx++
	}
	for orderIdx := 1; orderIdx <= len(priorityOrdersMap); orderIdx++ {
		orderColumn := priorityOrdersMap[orderIdx][0]
		orderDirection := priorityOrdersMap[orderIdx][1]
		if orderByStatement.Len() > 0 {
			orderByStatement.WriteString(",")
		}
		orderByStatement.WriteString(orderColumn)
		orderByStatement.WriteString(" ")
		orderByStatement.WriteString(orderDirection)
	}

	if orderByStatement.Len() > 0 {
		b.orderStatement.WriteString(orderByStatement.String())
	}

	return b
}
func (b *builder) Group(column string) Builder {
	if b.groupStatement.Len() > 0 {
		b.groupStatement.WriteString(",")
	}
	b.groupStatement.WriteString(column)
	b.groupStatement.WriteString(" ")
	return b
}
func (b *builder) Groups(columns []string) Builder {
	for _, item := range columns {
		if b.groupStatement.Len() > 0 {
			b.groupStatement.WriteString(",")
		}
		b.groupStatement.WriteString(item)
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
	var query strings.Builder
	if len(b.insert) > 0 {
		var columns strings.Builder
		var placeholder strings.Builder
		for key, value := range b.insert {
			if value != nil {
				if columns.Len() > 0 {
					columns.WriteString(",")
					placeholder.WriteString(",")
				}
				columns.WriteString(key)
				placeholder.WriteString("?")
				b.values = append(b.values, value)
			}
		}
		query.WriteString("INSERT INTO ")
		query.WriteString(b.source[0]["table"])
		query.WriteString("(")
		query.WriteString(columns.String())
		query.WriteString(") VALUES (")
		query.WriteString(placeholder.String())
		query.WriteString(");")
	} else if len(b.update) > 0 {
		var updates strings.Builder
		values := make([]interface{}, 0)
		for key, value := range b.update {
			if value != nil {
				if updates.Len() > 0 {
					updates.WriteString(",")
				}
				isAStatement := false
				if tmp, ok := value.(string); ok {
					isAStatement = strings.Contains(tmp, "`")
				}
				if isAStatement {
					updates.WriteString(key)
					updates.WriteString("=")
					updates.WriteString(value.(string))
				} else {
					updates.WriteString(key)
					updates.WriteString("=")
					updates.WriteString("?")
					values = append(values, value)
				}
			}
		}
		query.WriteString("UPDATE ")
		query.WriteString(b.source[0]["table"])
		query.WriteString(" SET ")
		query.WriteString(updates.String())
		if b.whereStatement.Len() > 0 {
			query.WriteString(" ")
			query.WriteString("WHERE ")
			query.WriteString(b.whereStatement.String())
			values = append(values, b.values...)
		}
		b.values = values
	} else if len(b.upsert) > 0 {
		var columns strings.Builder
		var placeholder strings.Builder
		temp := make(map[string]string)
		for key, value := range b.upsert {
			if value != nil {

				if str, ok := value.(string); ok && !strings.Contains(str, "`") {
					if columns.Len() > 0 {
						columns.WriteString(",")
						placeholder.WriteString(",")
					}
					columns.WriteString(key)
					placeholder.WriteString("?")
					b.values = append(b.values, value)
				} else {
					number, _ := regexp.Compile(`[a-zA-Z0-9 .]*$`)
					numbers := number.FindAllString(value.(string), -1)
					if len(numbers) > 0 {
						if columns.Len() > 0 {
							columns.WriteString(",")
							placeholder.WriteString(",")
						}
						columns.WriteString(key)
						placeholder.WriteString(numbers[0])
						temp[key] = value.(string)
					}
				}
			}
		}
		var updates strings.Builder
		for key, value := range b.upsert {
			if value != nil && temp[key] == "" {
				if updates.Len() > 0 {
					updates.WriteString(",")
				}
				updates.WriteString(key)
				updates.WriteString("=")
				updates.WriteString("?")
				b.values = append(b.values, value)
			}
		}
		if len(temp) > 0 {
			for key, value := range temp {
				if updates.Len() > 0 {
					updates.WriteString(",")
				}
				updates.WriteString(key)
				updates.WriteString("=")
				updates.WriteString(value)
			}
		}
		query.WriteString("INSERT INTO ")
		query.WriteString(b.source[0]["table"])
		query.WriteString("(")
		query.WriteString(columns.String())
		query.WriteString(") VALUES (")
		query.WriteString(placeholder.String())
		query.WriteString(") ON DUPLICATE KEY UPDATE ")
		query.WriteString(updates.String())
		query.WriteString(";")
	} else if b.delete {
		query.WriteString("DELETE ")
		query.WriteString("FROM ")
		query.WriteString(b.source[0]["table"])
		query.WriteString("WHERE ")
		query.WriteString(b.whereStatement.String())
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
			if b.next != 0 {
				query.WriteString(" AND ")
				direction := "asc"
				for _, value := range b.orderMap {
					direction = value
					break
				}
				if direction == "asc" {
					query.WriteString(fmt.Sprintf(" `id` > %d", b.next))
				} else {
					query.WriteString(fmt.Sprintf(" `id` < %d", b.next))
				}
			}
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
		if b.page != 0 && b.next == 0 {
			query.WriteString(" ")
			query.WriteString(fmt.Sprintf("OFFSET %d ", b.page*b.size))
		}
	}

	return query.String(), b.values
}
