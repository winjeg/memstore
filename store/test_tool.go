package store

import (
	"reflect"
	"testing"
)

func AssertEqual(t *testing.T, a, b interface{}) {
	if a != b {
		t.FailNow()
	}
}
func AssertNil(t *testing.T, v interface{}) {
	if v == nil {
		return
	}
	if reflect.ValueOf(v).IsNil() {
		return
	}
	t.FailNow()
}
