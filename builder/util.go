package builder

import (
	"strings"
	"time"
	"unicode"

	"golang.org/x/text/cases"
	"golang.org/x/text/language"
)

type Scanner func(args ...interface{}) error

func ResolveColumnName(column string) string {
	var builder strings.Builder
	var prev rune
	caser := cases.Title(language.Und, cases.NoLower)
	column = caser.String(column)
	containsDot := strings.Contains(column, ".")
	if !containsDot {
		builder.WriteRune('`')
	}
	for _, curr := range column {
		if prev >= 'a' && prev <= 'z' && curr >= 'A' && curr <= 'Z' {
			builder.WriteString("_")
			builder.WriteRune(unicode.ToLower(curr))
		} else {
			builder.WriteRune(unicode.ToLower(curr))
		}
		if curr == '.' {
			builder.WriteRune('`')
		}
		prev = curr
	}
	builder.WriteRune('`')
	return builder.String()
}
func ResolveColumnNameCollections(columns []string) []string {
	var results []string
	for _, c := range columns {
		results = append(results, ResolveColumnName(c))
	}
	return results
}
func ResolveColumnNameMaps(columns []map[string]interface{}) []map[string]interface{} {
	formatted := make([]map[string]interface{}, 0)
	for _, item := range columns {
		tmp := make(map[string]interface{})
		for key, val := range item {
			column := ResolveColumnName(key)
			tmp[column] = val
		}
		formatted = append(formatted, tmp)
	}
	return formatted
}
func ResolveColumnNameMap(column map[string]interface{}) map[string]interface{} {
	formatted := make(map[string]interface{}, 0)
	for key, val := range column {
		tmp := ResolveColumnName(key)
		formatted[tmp] = val
	}
	return formatted
}
func ResolveColumnNameMapInTime(column map[string][]time.Time) map[string][]time.Time {
	formatted := make(map[string][]time.Time, 0)
	for key, val := range column {
		tmp := ResolveColumnName(key)
		formatted[tmp] = val
	}
	return formatted
}
