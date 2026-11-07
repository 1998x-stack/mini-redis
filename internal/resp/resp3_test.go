package resp

import (
	"math"
	"reflect"
	"strconv"
	"strings"
	"testing"
)

func TestRESP3EncodingRicherTypes(t *testing.T) {
	value := MapValue([]MapEntry{
		{Key: BulkString("ok"), Value: Boolean(true)},
		{Key: BulkString("score"), Value: Double(1.5)},
		{Key: BulkString("members"), Value: Set([]Value{BulkString("a"), BulkString("b")})},
		{Key: BulkString("nothing"), Value: Null()},
	})
	got, err := EncodeValueProtocol(value, Protocol3)
	if err != nil {
		t.Fatal(err)
	}
	want := "%4\r\n" +
		"$2\r\nok\r\n#t\r\n" +
		"$5\r\nscore\r\n,1.5\r\n" +
		"$7\r\nmembers\r\n~2\r\n$1\r\na\r\n$1\r\nb\r\n" +
		"$7\r\nnothing\r\n_\r\n"
	if string(got) != want {
		t.Fatalf("RESP3 encoding=%q want=%q", got, want)
	}
}

func TestRESP2DowngradesRESP3Types(t *testing.T) {
	value := MapValue([]MapEntry{
		{Key: BulkString("ok"), Value: Boolean(true)},
		{Key: BulkString("score"), Value: Double(1.5)},
		{Key: BulkString("members"), Value: Set([]Value{BulkString("a"), BulkString("b")})},
	})
	got, err := EncodeValueProtocol(value, Protocol2)
	if err != nil {
		t.Fatal(err)
	}
	want := "*6\r\n" +
		"$2\r\nok\r\n:1\r\n" +
		"$5\r\nscore\r\n$3\r\n1.5\r\n" +
		"$7\r\nmembers\r\n*2\r\n$1\r\na\r\n$1\r\nb\r\n"
	if string(got) != want {
		t.Fatalf("RESP2 downgrade=%q want=%q", got, want)
	}
}

func TestParseValueFragmentedNestedRESP3(t *testing.T) {
	want := Push([]Value{
		BulkString("notice"),
		MapValue([]MapEntry{
			{Key: BulkString("ready"), Value: Boolean(true)},
			{Key: BulkString("lag"), Value: Double(2.25)},
		}),
		Set([]Value{Integer(1), Integer(2)}),
	})
	payload, err := EncodeValueProtocol(want, Protocol3)
	if err != nil {
		t.Fatal(err)
	}
	for cut := 0; cut < len(payload); cut++ {
		_, consumed, complete, err := ParseValue(payload[:cut])
		if err != nil {
			t.Fatalf("cut=%d err=%v", cut, err)
		}
		if complete || consumed != 0 {
			t.Fatalf("cut=%d complete=%v consumed=%d", cut, complete, consumed)
		}
	}
	got, consumed, complete, err := ParseValue(payload)
	if err != nil || !complete {
		t.Fatalf("complete=%v consumed=%d err=%v", complete, consumed, err)
	}
	if consumed != len(payload) {
		t.Fatalf("consumed=%d want=%d", consumed, len(payload))
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got=%#v want=%#v", got, want)
	}
}

func TestRESP3DoubleSpecialValues(t *testing.T) {
	for _, tc := range []struct {
		value float64
		wire  string
	}{
		{math.Inf(1), ",inf\r\n"},
		{math.Inf(-1), ",-inf\r\n"},
		{math.NaN(), ",nan\r\n"},
	} {
		encoded, err := EncodeValueProtocol(Double(tc.value), Protocol3)
		if err != nil {
			t.Fatal(err)
		}
		if string(encoded) != tc.wire {
			t.Fatalf("wire=%q want=%q", encoded, tc.wire)
		}
	}
}

func TestParseValueRejectsOversizedBlobAndAggregateLengths(t *testing.T) {
	maxInt := int(^uint(0) >> 1)
	cases := []struct {
		name string
		wire string
	}{
		{name: "blob integer overflow", wire: "$" + strconv.Itoa(maxInt) + "\r\n"},
		{name: "blob above limit", wire: "$" + strconv.Itoa(maxValueBlobBytes+1) + "\r\n"},
		{name: "array above limit", wire: "*" + strconv.Itoa(maxValueContainerElements+1) + "\r\n"},
		{name: "set above limit", wire: "~" + strconv.Itoa(maxValueContainerElements+1) + "\r\n"},
		{name: "push above limit", wire: ">" + strconv.Itoa(maxValueContainerElements+1) + "\r\n"},
		{name: "map above limit", wire: "%" + strconv.Itoa(maxValueContainerElements+1) + "\r\n"},
		{name: "array integer overflow", wire: "*" + strconv.Itoa(maxInt) + "\r\n"},
		{name: "map integer overflow", wire: "%" + strconv.Itoa(maxInt) + "\r\n"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, _, complete, err := ParseValue([]byte(tc.wire))
			if err == nil {
				t.Fatal("expected oversized length to be rejected")
			}
			if complete {
				t.Fatal("oversized length must not be reported as a complete value")
			}
		})
	}
}

func TestParseValueAcceptsContainerLimitAndValidIncompleteBlob(t *testing.T) {
	wire := "*" + strconv.Itoa(maxValueContainerElements) + "\r\n" +
		strings.Repeat("_\r\n", maxValueContainerElements)
	value, consumed, complete, err := ParseValue([]byte(wire))
	if err != nil || !complete || consumed != len(wire) {
		t.Fatalf("container boundary complete=%v consumed=%d err=%v", complete, consumed, err)
	}
	if len(value.Array) != maxValueContainerElements {
		t.Fatalf("container elements=%d want=%d", len(value.Array), maxValueContainerElements)
	}

	_, consumed, complete, err = ParseValue([]byte("$3\r\nab"))
	if err != nil || complete || consumed != 0 {
		t.Fatalf("valid incomplete blob complete=%v consumed=%d err=%v", complete, consumed, err)
	}
}
