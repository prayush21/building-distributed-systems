package replay

import "testing"

func TestParseSize(t *testing.T) {
	tests := []struct {
		in       string
		wantB    int64
		wantFrac float64
		wantErr  bool
	}{
		{in: "1gb", wantB: 1 << 30},
		{in: "100mb", wantB: 100 << 20},
		{in: "512kb", wantB: 512 << 10},
		{in: "1TB", wantB: 1 << 40},
		{in: "1048576", wantB: 1048576},
		{in: "1000", wantB: 1000},
		{in: " 2GB ", wantB: 2 << 30},
		{in: "1.5gb", wantB: 1610612736},
		{in: "0.1x", wantFrac: 0.1},
		{in: "0.001x", wantFrac: 0.001},
		{in: "2x", wantFrac: 2},
		// libCacheSim spells a fractional working set as a bare decimal.
		{in: "0.1", wantFrac: 0.1},
		{in: "0.001", wantFrac: 0.001},
		// Ambiguous: 1.5 could be bytes or a fraction. Refuse rather than guess.
		{in: "1.5", wantErr: true},
		{in: "", wantErr: true},
		{in: "0", wantErr: true},
		{in: "-5", wantErr: true},
		{in: "banana", wantErr: true},
		{in: "0x", wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.in, func(t *testing.T) {
			got, err := ParseSize(tc.in)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("ParseSize(%q) = %+v, want error", tc.in, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseSize(%q): %v", tc.in, err)
			}
			if got.Bytes != tc.wantB {
				t.Errorf("Bytes = %d, want %d", got.Bytes, tc.wantB)
			}
			if got.Fraction != tc.wantFrac {
				t.Errorf("Fraction = %v, want %v", got.Fraction, tc.wantFrac)
			}
			if got.Spec != tc.in {
				t.Errorf("Spec = %q, want %q: results must record what was asked for", got.Spec, tc.in)
			}
		})
	}
}

func TestSizeResolve(t *testing.T) {
	abs, err := ParseSize("1mb")
	if err != nil {
		t.Fatal(err)
	}
	if got, _ := abs.Resolve(999); got != 1<<20 {
		t.Errorf("absolute Resolve = %d, want %d (working set must be ignored)", got, 1<<20)
	}

	frac, err := ParseSize("0.1x")
	if err != nil {
		t.Fatal(err)
	}
	if got, _ := frac.Resolve(10000); got != 1000 {
		t.Errorf("fractional Resolve = %d, want 1000", got)
	}
	if _, err := frac.Resolve(0); err == nil {
		t.Error("resolving a fraction against an empty working set should fail")
	}
	if _, err := frac.Resolve(5); err == nil {
		t.Error("a fraction that rounds to zero bytes should fail, not silently become 0")
	}
}
