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

func TestParseCommandWaitsForPartialFrame(t *testing.T) {
	full := EncodeCommand([]string{"SET", "key", "value"})
	for i := 1; i < len(full); i++ {
		argv, consumed, complete, err := ParseCommand(full[:i])
		if err != nil {
			t.Fatalf("prefix %d returned error: %v", i, err)
		}
		if complete {
			t.Fatalf("prefix %d unexpectedly complete: argv=%v consumed=%d", i, argv, consumed)
		}
		if consumed != 0 {
			t.Fatalf("prefix %d consumed=%d; partial frames must remain buffered", i, consumed)
		}
	}
}

func TestParseCommandLeavesPipelineBytes(t *testing.T) {
	first := EncodeCommand([]string{"PING"})
	second := EncodeCommand([]string{"GET", "name"})
	data := append(append([]byte{}, first...), second...)

	argv, consumed, complete, err := ParseCommand(data)
	if err != nil {
		t.Fatal(err)
	}
	if !complete || consumed != len(first) {
		t.Fatalf("complete=%v consumed=%d want=%d", complete, consumed, len(first))
	}
	if len(argv) != 1 || argv[0] != "PING" {
		t.Fatalf("argv=%v", argv)
	}

	argv, consumed2, complete, err := ParseCommand(data[consumed:])
	if err != nil {
		t.Fatal(err)
	}
	if !complete || consumed2 != len(second) || len(argv) != 2 || argv[0] != "GET" || argv[1] != "name" {
		t.Fatalf("second argv=%v consumed=%d complete=%v", argv, consumed2, complete)
	}
}

func TestEncodeValue(t *testing.T) {
	got, err := EncodeValue(Array([]Value{BulkString("hello"), Integer(7)}))
	if err != nil {
		t.Fatal(err)
	}
	want := "*2\r\n$5\r\nhello\r\n:7\r\n"
	if string(got) != want {
		t.Fatalf("got=%q want=%q", got, want)
	}
}
