package paging

type Request struct {
	Page    int    `query:"page" json:"page,omitempty"`
	Next    int64  `query:"next" json:"next,omitempty"`
	Size    int    `query:"size" json:"size,omitempty"`
	Orderby string `param:"orderBy" query:"orderBy" json:"orderBy"`
}
