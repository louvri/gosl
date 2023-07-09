package builder

type Condition struct {
	Operator string
	Key      string
	OtherKey string
	Value    interface{}
}
