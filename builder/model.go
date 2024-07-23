package builder

import (
	"time"

	"github.com/louvri/gosl/transformer"
)

type QueryParams struct {
	Object       interface{}
	In           map[string]interface{}
	Notin        map[string]interface{}
	Conditions   []Condition
	Next         int64
	Page         int
	Size         int
	Orderby      []OrderBy
	Groupby      []string
	ColumnFilter []string
	Priority     []string
	Merge        *Merge
	BetweenTime  map[string][]time.Time
	IsDistinct   bool
}

func (q *QueryParams) Clone() QueryParams {
	in := make(map[string]interface{})
	for key, val := range q.In {
		in[key] = val
	}
	notin := make(map[string]interface{})
	for key, val := range q.Notin {
		notin[key] = val
	}
	conditions := make([]Condition, 0)
	conditions = append(conditions, q.Conditions...)

	orderby := make([]OrderBy, 0)
	for _, item := range q.Orderby {
		orderby = append(orderby, OrderBy{
			Column:    item.Column,
			Direction: item.Direction,
			Fields:    item.Fields,
		})
	}
	groupby := make([]string, 0)
	groupby = append(groupby, q.Groupby...)

	columnfilters := make([]string, 0)
	columnfilters = append(columnfilters, q.ColumnFilter...)

	betweentime := make(map[string][]time.Time)
	for key, value := range q.BetweenTime {
		betweentime[key] = make([]time.Time, 0)
		betweentime[key] = append(betweentime[key], value...)
	}
	return QueryParams{
		Object:       q.Object,
		In:           in,
		Notin:        notin,
		Conditions:   conditions,
		Next:         q.Next,
		Page:         q.Page,
		Size:         q.Size,
		Orderby:      orderby,
		Groupby:      groupby,
		ColumnFilter: columnfilters,
		BetweenTime:  betweentime,
		Merge: &Merge{
			Type:      q.Merge.Type,
			Track:     q.Merge.Track,
			Operation: q.Merge.Operation,
		},
	}
}

type MergeOperation int

const (
	Identifier MergeOperation = iota
	Statement
)

type Merge struct {
	Type           transformer.Type
	Track          string //identifier to check duplicates - or condition
	Operation      MergeOperation
	ShouldContinue func(data interface{}) bool
}

type OrderBy struct {
	Column    string
	Direction string
	Fields    []string
}
