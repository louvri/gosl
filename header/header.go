package header

type Request struct {
	Ref          string
	Token        string
	MachineToken string
	Timediff     int
	Timestamp    int64
	Origin       string
	Actor        string
}
