package transformer

type Type int64

const (
	Object Type = iota
	String
	Int
	Map
)

type Transformer struct {
	Type      Type
	Extract   func() interface{}
	Count     func(data interface{})
	Store     func(data interface{}) error
	Transform func(data interface{}) (interface{}, error)
	Iterate   func(f func(id string, kind string, data interface{}) error) error
}
