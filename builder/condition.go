package builder

type Condition struct {
	Operator string
	Key      string
	Value    interface{}
}

// Expr is a raw SQL fragment written into the statement unbound,
// e.g. Expr("`stock` - 1") or Expr("lower(`key`)").
// Every other value, including a plain string, is bound as a parameter.
// Upsert writes it raw on both sides, so on a new row a column it refers to
// reads that column's default (MySQL): `stock` - 1 inserts default - 1.
// Never build an Expr from user input.
type Expr string
