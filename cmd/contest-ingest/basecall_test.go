package main

import "testing"

// A portable designator can put the callsign on either side of the slash, or in the
// middle, and both sides can be present at once. The parser used to keep everything
// before the first slash, which is right for KI7MT/KP4 and wrong for LX/ON9TT -- and
// being wrong meant the QSO was SKIPPED, not mis-parsed, because parseQSOLine could
// not find a their_call. Systematically the DX.
func TestBaseCall(t *testing.T) {
	for _, c := range []struct{ in, want string }{
		// plain
		{"KI7MT", "KI7MT"},
		{"ON9TT", "ON9TT"},

		// prefix form -- call is AFTER the slash
		{"LX/ON9TT", "ON9TT"},
		{"9A/S57GM", "S57GM"},
		{"EA5/RV2A", "RV2A"},
		{"E7/Z35M", "Z35M"},
		{"WP4/K0BBC", "K0BBC"},
		{"OH0/DL1ABC", "DL1ABC"},
		{"KH0/KI7MT", "KI7MT"},
		{"F/ON4ABC", "ON4ABC"},

		// suffix form, DXCC prefix -- call is BEFORE
		{"KI7MT/KP4", "KI7MT"},
		{"K1ABC/VP9", "K1ABC"},

		// suffix form, qualifier -- call is BEFORE
		{"DL2AW/P", "DL2AW"},
		{"W1ABC/MM", "W1ABC"},
		{"W1ABC/AM", "W1ABC"},
		{"KI7MT/QRP", "KI7MT"},
		{"K1ABC/M", "K1ABC"},
		{"K1ABC/4", "K1ABC"}, // call-area change

		// both at once -- call is in the MIDDLE
		{"PA/DL2AW/P", "DL2AW"},
		{"LX/ON9TT/P", "ON9TT"},
		{"KH0/KI7MT/QRP", "KI7MT"},

		// special-event and commemorative calls -- long suffixes
		{"SN0MARCONI", "SN0MARCONI"},
		{"HG24TISZA", "HG24TISZA"},
		{"OH100SRAL", "OH100SRAL"},
		{"DL60RRDXA", "DL60RRDXA"},
		{"LX/SN0MARCONI/P", "SN0MARCONI"},

		// not callsigns at all
		{"", ""},
		{"P", ""},
		{"/", ""},
		{"599", ""},
		{"ARI", ""},
	} {
		if got := baseCall(c.in); got != c.want {
			t.Errorf("baseCall(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

// When both components are valid callsigns the form is genuinely ambiguous -- a human
// cannot tell either. Length breaks it: the operator's call is longer than the DXCC
// prefix it is operating under. This only decides WHICH component is reported; the
// raw field is what gets stored either way, so a wrong tie-break cannot corrupt bronze.
func TestBaseCallAmbiguousPrefixIsAlsoACall(t *testing.T) {
	if got := baseCall("VP2E/K1ABC"); got != "K1ABC" {
		t.Errorf("baseCall(VP2E/K1ABC) = %q, want K1ABC", got)
	}
}

// The station is itself however it spells itself. A log whose header says KI7MT and
// whose QSO line says KI7MT/KP4 must not treat its own station as the one worked.
func TestSelfNotTakenAsWorkedStation(t *testing.T) {
	// QSO: freq mode date time mycall rst exch theircall rst exch
	f := []string{"QSO:", "14025", "CW", "2024-07-13", "1200", "KI7MT/KP4", "599", "14", "ON9TT", "599", "28"}
	q, err := parseQSOLine(f, "KI7MT")
	if err != nil {
		t.Fatalf("parseQSOLine: %v", err)
	}
	if q.Call2 != "ON9TT" {
		t.Errorf("Call2 = %q, want ON9TT (the station's own call in portable form was taken as the worked station)", q.Call2)
	}
}

// A prefix-form worked station must produce a QSO rather than an error, and must be
// stored verbatim -- bronze is a faithful ingest, so the designator is not normalised away.
func TestPrefixFormWorkedStationIsKeptVerbatim(t *testing.T) {
	f := []string{"QSO:", "14025", "CW", "2024-07-13", "1200", "KI7MT", "599", "14", "LX/ON9TT", "599", "28"}
	q, err := parseQSOLine(f, "KI7MT")
	if err != nil {
		t.Fatalf("parseQSOLine returned %v -- this is the QSO that used to be skipped", err)
	}
	if q.Call2 != "LX/ON9TT" {
		t.Errorf("Call2 = %q, want LX/ON9TT stored verbatim", q.Call2)
	}
}
