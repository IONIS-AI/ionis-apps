package main

import (
	"bytes"
	"encoding/json"
	"testing"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

type msg struct {
	topic   string
	payload []byte
}

func (m msg) Duplicate() bool   { return false }
func (m msg) Qos() byte         { return 0 }
func (m msg) Retained() bool    { return false }
func (m msg) Topic() string     { return m.topic }
func (m msg) MessageID() uint16 { return 0 }
func (m msg) Payload() []byte   { return m.payload }
func (m msg) Ack()              {}

var _ mqtt.Message = msg{}

// Whatever arrives, the original bytes come back out of the written line.
func TestPayloadBytesRecoverable(t *testing.T) {
	cases := [][]byte{
		[]byte(`{"sq":73113037412,"f":3574299,"md":"FT8","rp":-10,"sc":"EA4BPO","rl":"FN41fl75","b":"80m"}`), // real, compact
		[]byte(`{"sq": 1, "sc": "K1ABC"}`), // valid JSON with spaces: must not be compacted
		[]byte(`not json at all <&>`),      // invalid: kept as text
	}
	for _, p := range cases {
		c := &capture{lines: make(chan record, 1)}
		c.onMessage(nil, msg{"pskr/filter/v2/x", p})
		r := <-c.lines
		var buf bytes.Buffer
		enc := json.NewEncoder(&buf)
		enc.SetEscapeHTML(false)
		if err := enc.Encode(r); err != nil {
			t.Fatal(err)
		}
		var back struct {
			Payload     json.RawMessage `json:"payload"`
			PayloadText *string         `json:"payload_text"`
		}
		if err := json.Unmarshal(buf.Bytes(), &back); err != nil {
			t.Fatal(err)
		}
		got := []byte(back.Payload)
		if back.PayloadText != nil {
			got = []byte(*back.PayloadText)
		}
		if !bytes.Equal(got, p) {
			t.Errorf("payload %q came back as %q", p, got)
		}
	}
}

// A full buffer is counted for an event line, never silently lost.
func TestDropIsCounted(t *testing.T) {
	c := &capture{lines: make(chan record)} // unbuffered, no reader: every send drops
	c.onMessage(nil, msg{"t", []byte(`{}`)})
	c.onMessage(nil, msg{"t", []byte(`{}`)})
	if c.dropped.Load() != 2 || c.stats.dropped.Load() != 2 {
		t.Errorf("dropped %d / %d, want 2 / 2", c.dropped.Load(), c.stats.dropped.Load())
	}
}
