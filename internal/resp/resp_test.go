package resp

import (
	"bytes"
	"reflect"
	"testing"
)

func TestReadCommand(t *testing.T) {
	raw := EncodeCommand([]string{"SET", "hello", "world"})
	got, err := NewReader(bytes.NewReader(raw)).ReadCommand()
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"SET", "hello", "world"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %#v want %#v", got, want)
	}
}

func TestWriteValues(t *testing.T) {
	var b bytes.Buffer
	w := NewWriter(&b)
	if err := w.WriteValue(Array([]Value{BulkString("a"), Integer(2), NullBulkString()})); err != nil {
		t.Fatal(err)
	}
	want := "*3\r\n$1\r\na\r\n:2\r\n$-1\r\n"
	if b.String() != want {
		t.Fatalf("got %q want %q", b.String(), want)
	}
}
