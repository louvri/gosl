package custom_error

import (
	"fmt"
	"strings"
)

type kind string

const (
	SERVER             kind = "server"
	DUPLICATE          kind = "duplicate"
	NOT_VALID          kind = "not_valid"
	NOT_ASSIGNED       kind = "not_assigned"
	NOT_FOUND          kind = "not_found"
	NOT_MATCH          kind = "not_match"
	NOT_ACCESSIBLE     kind = "not_accessible"
	NOT_CORRECT_FORMAT kind = "not_correct_format"
)

func Extract(err error) map[string]string {
	keyvals := make(map[string]string)
	msg := strings.Split(err.Error(), ":")
	msg[1] = strings.ReplaceAll(msg[1], " = ", "=")
	items := strings.Split(msg[1], "=")
	keyvals["code"] = strings.ReplaceAll(items[1], " desc", "")
	keyvals["desc"] = items[2]
	return keyvals
}
func New(code kind, desc string) error {
	return fmt.Errorf("%s:%s", code, desc)
}
