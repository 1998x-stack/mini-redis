package store

import "testing"

func TestStringObjectEncodingPreservesLogicalValue(t *testing.T) {
	tests := []struct {
		input    string
		encoding ObjectEncoding
	}{
		{"0", EncodingInt},
		{"42", EncodingInt},
		{"-42", EncodingInt},
		{"001", EncodingRaw},
		{"+1", EncodingRaw},
		{"-0", EncodingRaw},
		{"9223372036854775808", EncodingRaw},
		{"hello", EncodingRaw},
	}
	for _, tt := range tests {
		obj := NewString(tt.input)
		if obj.Type != ObjectString || obj.Encoding != tt.encoding {
			t.Fatalf("NewString(%q) type=%v encoding=%v want string/%v", tt.input, obj.Type, obj.Encoding, tt.encoding)
		}
		got, ok := obj.StringValue()
		if !ok || got != tt.input {
			t.Fatalf("NewString(%q).StringValue()=(%q,%v)", tt.input, got, ok)
		}
	}
}
