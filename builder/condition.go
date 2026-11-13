package builder

type Condition struct {
	Operator string
	Key      string
	Value    interface{}
}

// Expr is a raw SQL fragment written into the statement unbound,
// e.g. Expr("`stock` - 1") or Expr("lower(`key`)").
// Every other value, including a plain string, is bound as a parameter.
// Upsert writes it raw on both sides. MySQL evaluates the columns left to
// right, so a column the Expr refers to reads the value set earlier in the
// same row, or on INSERT that column's default when it is not set earlier:
// on a new row `stock` - 1 inserts default - 1. Columns are written in the
// order of the columns argument, or sorted by name when none is given; pass
// columns when an Expr refers to another column in the same map.
// Never build an Expr from user input.
type Expr string
