package builder

import (
	"reflect"
	"strings"
)

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
	} else if tmp, ok := condition.Value.(string); ok && strings.Contains(tmp, "`") {
		s.WriteString(tmp)
		return s.String(), nil
	} else {
		s.WriteString("?")
		return s.String(), condition.Value
	}
}
