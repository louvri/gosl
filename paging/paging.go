package paging

import (
	"strconv"

	custom_error "github.com/louvri/gosl/custom_error"
)

type Request struct {
	Page    int    `query:"page" json:"page,omitempty"`
	Next    int64  `query:"next" json:"next,omitempty"`
	Size    int    `query:"size" json:"size,omitempty"`
	Orderby string `param:"orderBy" query:"orderBy" json:"orderBy"`
}

func (b *Request) Parse(data map[string]interface{}) error {
	if data["page"] != nil {
		if tmp, ok := data["page"].(string); ok {
			if tmp != "" {
				converted, err := strconv.Atoi(tmp)
				if err != nil {
					return custom_error.New(custom_error.NOT_CORRECT_FORMAT, "page must be number")
				}
				b.Page = converted
			}
		} else if tmp, ok := data["page"].(int); ok {
			b.Page = tmp
		} else {
			return custom_error.New(custom_error.NOT_CORRECT_FORMAT, "page must be number")
		}
	}
	if data["size"] != nil {
		if tmp, ok := data["size"].(string); ok {
			if tmp != "" {
				converted, err := strconv.Atoi(tmp)
				if err != nil {
					return custom_error.New(custom_error.NOT_CORRECT_FORMAT, "size must be number")
				}
				b.Size = converted
			}
		} else if tmp, ok := data["size"].(float64); ok {
			b.Size = int(tmp)
		} else {
			return custom_error.New(custom_error.NOT_CORRECT_FORMAT, "size must be number")
		}
	}
	if data["next"] != nil {
		if tmp, ok := data["next"].(string); ok {
			if tmp != "" {
				converted, err := strconv.ParseInt(tmp, 10, 64)
				if err != nil {
					return custom_error.New(custom_error.NOT_CORRECT_FORMAT, "next must be number")
				}
				b.Next = converted
			}
		} else if tmp, ok := data["next"].(float64); ok {
			b.Next = int64(tmp)
		} else {
			return custom_error.New(custom_error.NOT_CORRECT_FORMAT, "next must be number")
		}
	}
	if data["orderBy"] != nil {
		if tmp, ok := data["orderBy"].(string); ok {
			if tmp != "" {
				b.Orderby = tmp
			}
		} else {
			return custom_error.New(custom_error.NOT_CORRECT_FORMAT, "orderBy must be string")
		}
	}
	return nil
}
