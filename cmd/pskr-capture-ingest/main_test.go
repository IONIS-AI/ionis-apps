package main

import (
	"bytes"
	"compress/gzip"
	"os"
	"path/filepath"
	"testing"
)

// A real message line as pskr-capture wrote it on 2026-09-26.
const msgLine = `{"rx":"2026-09-26T14:11:29.9Z","topic":"pskr/filter/v2/80m/FT8/EA4BPO/N8LI/IN80/FN41/281/291","payload":{"sq":73113037412,"f":3574299,"md":"FT8","rp":-10,"t":1790386200,"t_tx":1790386185,"sc":"EA4BPO","sl":"IN80ej","rc":"N8LI","rl":"FN41fl75","sa":281,"ra":291,"b":"80m"}}`

func TestMessageLine(t *testing.T) {
	r := parseLine(7, []byte(msgLine))
	if r.parseErr != "" {
		t.Fatal(r.parseErr)
	}
	if *r.sq != 73113037412 || *r.f != 3574299 || *r.rp != -10 || r.md != "FT8" || r.sc != "EA4BPO" || r.rl != "FN41fl75" ||
		*r.sa != 281 || *r.ra != 291 || r.b != "80m" || r.t.Unix() != 1790386200 || r.ttx.Unix() != 1790386185 {
		t.Errorf("fields wrong: %+v", r)
	}
	if r.bandADIF == nil || *r.bandADIF != 103 {
		t.Errorf("band_adif %v, want 103 (80m)", r.bandADIF)
	}
	if len(r.extra) != 0 || r.lineNo != 7 {
		t.Errorf("extra %v line %d", r.extra, r.lineNo)
	}
}

func TestEventLine(t *testing.T) {
	r := parseLine(1, []byte(`{"rx":"2026-09-26T14:11:29.55Z","event":"dropped","detail":"write buffer full","count":42}`))
	if r.parseErr != "" || r.event != "dropped" || *r.count != 42 || r.sq != nil {
		t.Errorf("event parsed wrong: %+v", r)
	}
}

// A field the feed adds later, and a known field arriving with a new type, are kept.
func TestNothingDropped(t *testing.T) {
	r := parseLine(1, []byte(`{"rx":"2026-09-26T14:11:29Z","topic":"x","payload":{"sq":1,"new_field":[1,2],"sa":"ITA"}}`))
	if r.parseErr != "" {
		t.Fatal(r.parseErr)
	}
	if r.extra["new_field"] != "[1,2]" || r.extra["sa"] != `"ITA"` || r.sa != nil {
		t.Errorf("extra %v sa %v, want new_field and the string sa kept in extra", r.extra, r.sa)
	}
}

func TestUnreadableLineKept(t *testing.T) {
	r := parseLine(5, []byte(`{"rx":"2026-09-26T14:1`))
	if r.parseErr == "" || r.raw == "" {
		t.Errorf("cut line not kept: %+v", r)
	}
}

// A crashed hour: gzip cut mid-stream. Every readable line is returned, truncated=true.
func TestTruncatedFile(t *testing.T) {
	var full bytes.Buffer
	zw := gzip.NewWriter(&full)
	for i := 0; i < 2000; i++ {
		zw.Write([]byte(msgLine + "\n"))
	}
	zw.Close()
	cut := full.Bytes()[:full.Len()*2/3]
	p := filepath.Join(t.TempDir(), "capture-000000.jsonl.gz.partial")
	if err := os.WriteFile(p, cut, 0o644); err != nil {
		t.Fatal(err)
	}
	rows, trunc, err := parseFile(p)
	if err != nil || !trunc {
		t.Fatalf("err %v truncated %v, want nil true", err, trunc)
	}
	if len(rows) == 0 || len(rows) >= 2000 {
		t.Fatalf("%d rows from a cut file", len(rows))
	}
	for _, r := range rows[:len(rows)-1] {
		if r.parseErr != "" {
			t.Fatalf("line %d: %s", r.lineNo, r.parseErr)
		}
	}
}
