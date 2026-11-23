package builder

import (
	"regexp"
	"strings"
	"time"
	"unicode"

	"golang.org/x/text/cases"
	"golang.org/x/text/language"
)

type Scanner func(args ...interface{}) error

var identifier = regexp.MustCompile(`^[A-Za-z0-9_]+$`)

// quoteColumn backticks a column name, optionally prefixed by a table alias,
// and reports false when it is anything other than plain identifiers.
func quoteColumn(column string) (string, bool) {
	parts := strings.Split(strings.ReplaceAll(column, "`", ""), ".")
	if len(parts) > 2 {
		return "", false
	}
	for i, part := range parts {
		if !identifier.MatchString(part) {
			return "", false
		}
		parts[i] = "`" + part + "`"
	}
	return strings.Join(parts, "."), true
}

// orderDirection returns ASC or DESC; anything else falls back to ASC.
func orderDirection(direction string) string {
	if strings.EqualFold(strings.TrimSpace(direction), "desc") {
		return "DESC"
	}
	return "ASC"
}

func ResolveColumnName(column string) string {
	var builder strings.Builder
	var prev rune
	caser := cases.Title(language.Und, cases.NoLower)
	column = caser.String(column)
	if strings.Contains(column, "`") {
		return column
	}
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
func ResolveColumnNameWithoutBacktick(column string) string {
	var builder strings.Builder
	var prev rune
	caser := cases.Title(language.Und, cases.NoLower)
	column = caser.String(column)
	for _, curr := range column {
		if prev >= 'a' && prev <= 'z' && curr >= 'A' && curr <= 'Z' {
			builder.WriteString("_")
			builder.WriteRune(unicode.ToLower(curr))
		} else {
			builder.WriteRune(unicode.ToLower(curr))
		}
		prev = curr
	}
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
