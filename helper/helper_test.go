package helper

import "testing"

func TestHash32(t *testing.T) {
	t.Log(Hash32([]byte("Hello World")))
	t.Log(Hash32([]byte("Hello World1")))
	t.Log(Hash32([]byte("Hello World!!")))
	t.Log(Hash32([]byte("Hello World!!!")))
}
