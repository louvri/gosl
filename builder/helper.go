package builder

import (
	"reflect"
	"sort"
	"strings"
)

// sortedKeys orders the columns written when no columns are given, so the
// statement text is stable and an Expr sees the same earlier columns each time.
func sortedKeys(data map[string]interface{}) []string {
	keys := make([]string, 0, len(data))
	for key := range data {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func buildInStatement(prop string, data interface{}) (string, []interface{}) {
	var s strings.Builder
	var values []interface{}
	reflectItems := reflect.ValueOf(data)
	n := reflectItems.Len()
	if n <= 0 {
		return "", nil
	}
	if !strings.Contains(prop, "`") {
		s.WriteString("`")
		s.WriteString(prop)
		s.WriteString("`")
	} else {
		s.WriteString(prop)
	}
	s.WriteString(" IN (")
	for i := 0; i < n; i++ {
		if i > 0 {
			s.WriteString(",")
		}
		s.WriteString("?")
		values = append(values, reflectItems.Index(i).Interface())
	}
	s.WriteString(") ")
	return s.String(), values
}

func buildConditionStatement(condition Condition) (string, interface{}) {
	var s strings.Builder
	if !strings.Contains(condition.Key, "`") && !strings.Contains(condition.Key, "'$.") {
		tokens := strings.Split(condition.Key, ".")
		if len(tokens) == 2 {
			s.WriteString(tokens[0])
			s.WriteString(".")
			s.WriteString("`")
			s.WriteString(tokens[1])
			s.WriteString("`")
		} else {
			s.WriteString("`")
			s.WriteString(condition.Key)
			s.WriteString("` ")
		}
	} else {
		s.WriteString(condition.Key)
	}

	s.WriteString(condition.Operator)
	s.WriteString(" ")
	if condition.Value == nil || condition.Value == "null" {
		s.WriteString("null")
		return s.String(), nil
	} else if expr, ok := condition.Value.(Expr); ok {
		s.WriteString(string(expr))
		return s.String(), nil
	} else {
		s.WriteString("?")
		return s.String(), condition.Value
	}
}

func buildInsert(table string, data map[string]interface{}, columns []string) (string, []interface{}) {
	var query strings.Builder
	var fields strings.Builder
	var placeholder strings.Builder
	values := make([]interface{}, 0)
	build := func(key string, value interface{}) {
		if !strings.Contains(key, "`") {
			key = "`" + key + "`"
		}
		if value != nil {
			if fields.Len() > 0 {
				fields.WriteString(",")
				placeholder.WriteString(",")
			}
			fields.WriteString(key)
			placeholder.WriteString("?")
			values = append(values, value)
		} else {
			if fields.Len() > 0 {
				fields.WriteString(",")
				placeholder.WriteString(",")
			}
			placeholder.WriteString("NULL")
		}
	}
	if len(columns) > 0 {
		for _, column := range columns {
			key := column
			value := data[column]
			if value != nil {
				build(key, value)
			}
		}
	} else {
		for _, key := range sortedKeys(data) {
			build(key, data[key])
		}
	}
	query.WriteString("INSERT INTO ")
	query.WriteString(table)
	query.WriteString("(")
	query.WriteString(fields.String())
	query.WriteString(") VALUES (")
	query.WriteString(placeholder.String())
	query.WriteString(");")
	return query.String(), values
}

func buildUpdate(table string, data map[string]interface{}, columns []string) (string, []interface{}) {
	var query strings.Builder
	var placeholder strings.Builder
	values := make([]interface{}, 0)
	build := func(key string, value interface{}) {
		if !strings.Contains(key, "`") {
			key = "`" + key + "`"
		}
		if value != nil {
			if placeholder.Len() > 0 {
				placeholder.WriteString(",")
			}
			if expr, ok := value.(Expr); ok {
				placeholder.WriteString(key)
				placeholder.WriteString("=")
				placeholder.WriteString(string(expr))
			} else {
				placeholder.WriteString(key)
				placeholder.WriteString("=")
				placeholder.WriteString("?")
				values = append(values, value)
			}
		} else {
			if placeholder.Len() > 0 {
				placeholder.WriteString(",")
			}
			placeholder.WriteString(key)
			placeholder.WriteString("=")
			placeholder.WriteString("NULL")
		}
	}
	if len(columns) > 0 {
		for _, column := range columns {
			key := column
			value := data[column]
			if value != nil {
				build(key, value)
			}
		}
	} else {
		for _, key := range sortedKeys(data) {
			build(key, data[key])
		}
	}
	query.WriteString("UPDATE ")
	query.WriteString(table)
	query.WriteString(" SET ")
	query.WriteString(placeholder.String())
	return query.String(), values
}

func buildUpsert(table string, data map[string]interface{}, columns []string) (string, []interface{}) {
	var query strings.Builder
	var fields strings.Builder
	var insert strings.Builder
	var update strings.Builder
	insertValues := make([]interface{}, 0)
	updateValues := make([]interface{}, 0)
	buildInsert := func(key string, value interface{}) {
		if !strings.Contains(key, "`") {
			key = "`" + key + "`"
		}
		if value != nil {
			if _, ok := value.(Expr); !ok {
				if fields.Len() > 0 {
					fields.WriteString(",")
					insert.WriteString(",")
				}
				fields.WriteString(key)
				insert.WriteString("?")
				insertValues = append(insertValues, value)
			} else {
				strValue := string(value.(Expr))
				if fields.Len() > 0 {
					fields.WriteString(",")
					insert.WriteString(",")
				}
				fields.WriteString(key)
				insert.WriteString(strValue)
			}
		} else {
			if fields.Len() > 0 {
				fields.WriteString(",")
				insert.WriteString(",")
			}
			fields.WriteString(key)
			insert.WriteString("NULL")
		}
	}
	buildUpdate := func(key string, value interface{}) {
		if !strings.Contains(key, "`") {
			key = "`" + key + "`"
		}
		if value != nil {
			if update.Len() > 0 {
				update.WriteString(",")
			}
			if expr, ok := value.(Expr); ok {
				update.WriteString(key)
				update.WriteString("=")
				update.WriteString(string(expr))
			} else {
				update.WriteString(key)
				update.WriteString("=")
				update.WriteString("?")
				updateValues = append(updateValues, value)
			}
		} else {
			if update.Len() > 0 {
				update.WriteString(",")
			}
			update.WriteString(key)
			update.WriteString("=")
			update.WriteString("NULL")
		}
	}
	if len(columns) > 0 {
		for _, column := range columns {
			key := column
			value := data[column]
			if value != nil {
				buildInsert(key, value)
				buildUpdate(key, value)
			}
		}
	} else {
		for _, key := range sortedKeys(data) {
			buildInsert(key, data[key])
			buildUpdate(key, data[key])
		}
	}
	query.WriteString("INSERT INTO ")
	query.WriteString(table)
	query.WriteString("(")
	query.WriteString(fields.String())
	query.WriteString(") VALUES (")
	query.WriteString(insert.String())
	query.WriteString(") ON DUPLICATE KEY UPDATE ")
	query.WriteString(update.String())
	query.WriteString(";")
	output := make([]interface{}, 0)
	output = append(output, insertValues...)
	output = append(output, updateValues...)
	return query.String(), output
}
