package main

import "testing"

// A compound callsign can put the call on either side of the slash, or in the
// middle, and both sides can be present at once. The parser used to keep everything
// before the first slash, which is right for KI7MT/KP4 and wrong for LX/ON9TT -- and
// being wrong meant the QSO was SKIPPED, not mis-parsed, because parseQSOLine could
// not find a their_call. Systematically the DX.
func TestBaseCall(t *testing.T) {
	for _, c := range []struct{ in, want string }{
		// plain
		{"KI7MT", "KI7MT"},
		{"ON9TT", "ON9TT"},

		// location prefix -- call is AFTER the slash
		{"LX/ON9TT", "ON9TT"},
		{"9A/S57GM", "S57GM"},
		{"EA5/RV2A", "RV2A"},
		{"E7/Z35M", "Z35M"},
		{"WP4/K0BBC", "K0BBC"},
		{"OH0/DL1ABC", "DL1ABC"},
		{"KH0/KI7MT", "KI7MT"},
		{"F/ON4ABC", "ON4ABC"},

		// location suffix -- call is BEFORE
		{"KI7MT/KP4", "KI7MT"},
		{"K1ABC/VP9", "K1ABC"},

		// operating suffix (/P portable, /M mobile, /MM maritime, /AM aeronautical,
		// /QRP low power) and call-area change -- call is BEFORE
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
	q, err := parseQSOLine(f, "KI7MT", "CQ-WW-CW")
	if err != nil {
		t.Fatalf("parseQSOLine: %v", err)
	}
	if q.Call2 != "ON9TT" {
		t.Errorf("Call2 = %q, want ON9TT (the station's own call in compound form was taken as the worked station)", q.Call2)
	}
}

// A location-prefix worked station must produce a QSO rather than an error, and must
// be stored verbatim -- bronze is a faithful ingest, so the compound form is kept.
func TestPrefixFormWorkedStationIsKeptVerbatim(t *testing.T) {
	f := []string{"QSO:", "14025", "CW", "2024-07-13", "1200", "KI7MT", "599", "14", "LX/ON9TT", "599", "28"}
	q, err := parseQSOLine(f, "KI7MT", "CQ-WW-CW")
	if err != nil {
		t.Fatalf("parseQSOLine returned %v -- this is the QSO that used to be skipped", err)
	}
	if q.Call2 != "LX/ON9TT" {
		t.Errorf("Call2 = %q, want LX/ON9TT stored verbatim", q.Call2)
	}
}

// 7Q1 is a real, licensed Malawi callsign -- it ends in a digit, which the shape
// regex forbids, and it appears 2,461 times in one contest-year alone. The regex
// cannot be relaxed to allow a trailing digit, because then 599 and 37 match it and
// the wrong field becomes the worked station. So the row must survive positionally.
//
// Bronze is a faithful ingest. A callsign the parser does not recognise is not a
// reason to discard the QSO.
func TestOddCallsignDoesNotDropTheQSO(t *testing.T) {
	// freq mode date time mycall rst_s exch_s theircall rst_r exch_r
	f := []string{"QSO:", "28000", "PH", "2024-10-26", "1553", "CQ7K", "59", "14", "7Q1", "59", "37"}
	q, err := parseQSOLine(f, "CQ7K", "CQ-WW-SSB")
	if err != nil {
		t.Fatalf("parseQSOLine returned %v -- a real callsign the regex does not match must not drop the row", err)
	}
	if q.Call2 != "7Q1" {
		t.Errorf("Call2 = %q, want 7Q1 stored verbatim", q.Call2)
	}
	if q.Call1 != "CQ7K" {
		t.Errorf("Call1 = %q, want CQ7K", q.Call1)
	}
}

// The fallback must not fire when the scan already found the worked station, and
// must not invent one from an empty field.
func TestPositionalFallbackDoesNotOverrideOrInvent(t *testing.T) {
	// A normal line: the scan finds ON9TT at f[7]; fallback is irrelevant.
	f := []string{"QSO:", "14025", "CW", "2024-07-13", "1200", "KI7MT", "599", "14", "ON9TT", "599", "28"}
	q, err := parseQSOLine(f, "KI7MT", "CQ-WW-CW")
	if err != nil || q.Call2 != "ON9TT" {
		t.Fatalf("normal line: got %v / %+v", err, q)
	}

	// Exchange-shifted layout: the worked station is NOT at f[7]. The scan must win,
	// so the fallback cannot drag it back to the wrong column.
	g := []string{"QSO:", "14025", "CW", "2024-07-13", "1200", "KI7MT", "599", "MT", "USA", "ON9TT", "599", "ON"}
	q2, err := parseQSOLine(g, "KI7MT", "CQ-WW-CW")
	if err != nil {
		t.Fatalf("shifted layout: %v", err)
	}
	if q2.Call2 != "ON9TT" {
		t.Errorf("shifted layout: Call2 = %q, want ON9TT -- the scan should locate it, not position", q2.Call2)
	}
}

// THERE IS A CABRILLO TEMPLATE PER CONTEST. Sweepstakes carries a four-part exchange,
// so the received callsign is at f[9], not the generic f[7]:
//
//	CQ-WW  freq mo date time call rst exch      call rst exch t
//	SS     freq mo date time call nr p ck sec   call nr p ck sec
//
// A single hardcoded fallback index writes the SECTION into call_2 for every SS QSO
// whose callsign the shape test does not recognise. This is the regression guard.
func TestSweepstakesTemplateNotGenericTemplate(t *testing.T) {
	if got := specTheirCallIdx("ARRL-SS-CW"); got != 9 {
		t.Errorf("ARRL-SS-CW their_call index = %d, want 9", got)
	}
	if got := specTheirCallIdx("CQ-WW-CW"); got != 7 {
		t.Errorf("CQ-WW-CW their_call index = %d, want 7 (generic)", got)
	}
	if got := specTheirCallIdx("SOMETHING-NEW"); got != 7 {
		t.Errorf("unknown contest index = %d, want the generic 7", got)
	}

	// A real SS line: KA9FOX works WA9LEY. f[7] is the check "49", f[8] the section
	// "WI", f[9] the callsign. Give it a worked call the shape test cannot match, so
	// the fallback is what decides -- and prove it lands on the call, not the section.
	//   QSO: freq mo date time call nr p ck sec call nr p ck sec
	f := []string{"QSO:", "03539", "CW", "2018-11-05", "0233", "KA9FOX", "0001", "U", "49", "WI",
		"7Q1", "0264", "U", "63", "IL"}
	q, err := parseQSOLine(f, "KA9FOX", "ARRL-SS-CW")
	if err != nil {
		t.Fatalf("parseQSOLine: %v", err)
	}
	if q.Call2 == "WI" {
		t.Fatalf("Call2 = WI -- the section was stored as the worked station; the generic f[7] fallback was used for a Sweepstakes log")
	}
	if q.Call2 != "7Q1" {
		t.Errorf("Call2 = %q, want 7Q1", q.Call2)
	}
}
