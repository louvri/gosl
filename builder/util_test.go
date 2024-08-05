package builder

import (
	"strings"
	"testing"
)

func TestResolveColumnNameWithoutBacktick(t *testing.T) {
	result := ResolveColumnNameWithoutBacktick("testId")
	if strings.Contains(result, "`") {
		t.Fail()
	}
	t.Log(result)
}
