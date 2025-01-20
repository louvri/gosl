package builder

import (
	"time"
)

type QueryParams struct {
	Object       interface{}
	In           map[string]interface{}
	Notin        map[string]interface{}
	Conditions   []Condition
	Next         *Next
	Page         int
	Size         int
	Orderby      []OrderBy
	Groupby      []string
	ColumnFilter []string
	Priorities   []string
	Merge        *Merge
	BetweenTime  map[string][]time.Time
	IsDistinct   bool
	Name         string
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
			Track:          q.Merge.Track,
			Operation:      q.Merge.Operation,
			ShouldContinue: q.Merge.ShouldContinue,
		},
	}
}

type MergeOperation int

const (
	Statement MergeOperation = iota
	Identifier
)

type Merge struct {
	Track          string //identifier to check duplicates - or condition
	Operation      MergeOperation
	ShouldContinue func(data interface{}) bool
}

type OrderBy struct {
	Column    string
	Direction string
	Fields    []string
}

type Next struct {
	Column    string
	Direction string
	Value     interface{}
}
