package builder

type Condition struct {
	Operator string
	Key      string
	Value    interface{}
}

// Expr is a raw SQL fragment written into the statement unbound,
// e.g. Expr("`stock` - 1") or Expr("lower(`key`)").
// Every other value, including a plain string, is bound as a parameter.
// Never build an Expr from user input.
type Expr string
