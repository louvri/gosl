package builder

// Raw is a fragment of SQL that is interpolated into the generated statement
// verbatim, without being bound as a placeholder parameter.
//
// Use Raw only for library- or developer-controlled SQL expressions such as
// column references (`col`), function calls (COALESCE(`col`, 0)) or
// arithmetic (`col` + 1). Never wrap a value that originates from an
// untrusted source in Raw: any string received from a user or external
// system must be passed as a plain string so the builder binds it via a
// placeholder, otherwise the value becomes a SQL injection vector.
//
// Example (INSERT ... ON DUPLICATE KEY UPDATE counter = counter + 1):
//
//	builder.New().From("`t`").Upsert(map[string]interface{}{
//	    "`counter`": builder.Raw("`counter` + 1"),
//	})
type Raw string

// asRawSQL reports whether v is an opt-in raw SQL fragment. Only values of
// type Raw are treated as raw SQL. Plain strings – including strings that
// happen to contain a backtick – are always treated as data and bound via
// a placeholder by the caller.
func asRawSQL(v interface{}) (string, bool) {
	if r, ok := v.(Raw); ok {
		return string(r), true
	}
	return "", false
}
