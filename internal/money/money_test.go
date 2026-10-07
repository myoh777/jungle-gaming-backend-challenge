package money

import (
	"encoding/json"
	"errors"
	"math"
	"testing"
)

func TestParseNonNegative_Valid(t *testing.T) {
	cases := map[string]int64{
		"0":                    0,
		"0.00":                 0,
		"25.00":                2500,
		"25.5":                 2550,
		"25":                   2500,
		"0.01":                 1,
		"100.99":               10099,
		"92233720368547758.07": math.MaxInt64,
	}
	for in, want := range cases {
		m, err := ParseNonNegative(in, "BRL")
		if err != nil {
			t.Fatalf("%q: unexpected error %v", in, err)
		}
		if m.MinorUnits() != want || m.Currency() != BRL {
			t.Fatalf("%q: got %d %s, want %d BRL", in, m.MinorUnits(), m.Currency(), want)
		}
	}
}

func TestParseNonNegative_Invalid(t *testing.T) {
	cases := map[string]error{
		"":                     ErrInvalidAmount,
		" 1.00":                ErrInvalidAmount,
		"1.00 ":                ErrInvalidAmount,
		"NaN":                  ErrInvalidAmount,
		"Infinity":             ErrInvalidAmount,
		"-Infinity":            ErrNegativeAmount,
		"1e3":                  ErrInvalidAmount,
		"1.5E2":                ErrInvalidAmount,
		"+1.00":                ErrInvalidAmount,
		"-1.00":                ErrNegativeAmount,
		"1,00":                 ErrInvalidAmount,
		".50":                  ErrInvalidAmount,
		"5.":                   ErrInvalidAmount,
		"007.00":               ErrInvalidAmount,
		"1.001":                ErrScaleExceeded,
		"0.005":                ErrScaleExceeded,
		"92233720368547758.08": ErrOverflow,
		"99999999999999999999": ErrOverflow,
		"0x10":                 ErrInvalidAmount,
	}
	for in, want := range cases {
		_, err := ParseNonNegative(in, "BRL")
		if !errors.Is(err, want) {
			t.Fatalf("%q: got %v, want %v", in, err, want)
		}
	}
}

func TestParse_AllowsNegativeInternally(t *testing.T) {
	m, err := Parse("-1.50", "BRL")
	if err != nil || m.MinorUnits() != -150 || m.Amount() != "-1.50" {
		t.Fatalf("got %v %v", m, err)
	}
}

func TestParseCurrency(t *testing.T) {
	for _, bad := range []string{"", "brl", "BR", "XXX", "BRLL"} {
		if _, err := ParseCurrency(bad); !errors.Is(err, ErrUnsupportedCurrency) {
			t.Fatalf("%q: expected unsupported currency, got %v", bad, err)
		}
	}
	if _, err := ParseNonNegative("1.00", "XYZ"); !errors.Is(err, ErrUnsupportedCurrency) {
		t.Fatalf("expected unsupported currency, got %v", err)
	}
}

func TestZero(t *testing.T) {
	z, err := Zero(USD)
	if err != nil || !z.IsZero() || z.Currency() != USD || z.Amount() != "0.00" {
		t.Fatalf("got %v %v", z, err)
	}
}

func TestAddSubNegate(t *testing.T) {
	a := MustNew(1050, BRL)
	b := MustNew(250, BRL)
	sum, err := a.Add(b)
	if err != nil || sum.MinorUnits() != 1300 {
		t.Fatalf("add: %v %v", sum, err)
	}
	diff, err := b.Sub(a)
	if err != nil || diff.MinorUnits() != -800 || diff.Amount() != "-8.00" {
		t.Fatalf("sub: %v %v", diff, err)
	}
	neg, err := a.Negate()
	if err != nil || neg.MinorUnits() != -1050 {
		t.Fatalf("negate: %v %v", neg, err)
	}
	// Immutability: operands are unchanged.
	if a.MinorUnits() != 1050 || b.MinorUnits() != 250 {
		t.Fatal("operands mutated")
	}
}

func TestOverflow(t *testing.T) {
	max := MustNew(math.MaxInt64, BRL)
	one := MustNew(1, BRL)
	if _, err := max.Add(one); !errors.Is(err, ErrOverflow) {
		t.Fatalf("add overflow: %v", err)
	}
	minNeg := MustNew(-math.MaxInt64, BRL)
	if _, err := minNeg.Sub(one); !errors.Is(err, ErrOverflow) {
		t.Fatalf("sub overflow: %v", err)
	}
	if _, err := minNeg.Add(MustNew(-1, BRL)); !errors.Is(err, ErrOverflow) {
		t.Fatalf("negative add overflow: %v", err)
	}
	if _, err := New(math.MinInt64, BRL); !errors.Is(err, ErrOverflow) {
		t.Fatalf("MinInt64 must be rejected: %v", err)
	}
	neg, err := max.Negate()
	if err != nil || neg.MinorUnits() != -math.MaxInt64 {
		t.Fatalf("negate max: %v %v", neg, err)
	}
	if _, err := neg.Sub(max); !errors.Is(err, ErrOverflow) {
		t.Fatalf("sub to below range: %v", err)
	}
}

func TestCurrencyMismatch(t *testing.T) {
	brl := MustNew(100, BRL)
	usd := MustNew(100, USD)
	if _, err := brl.Add(usd); !errors.Is(err, ErrCurrencyMismatch) {
		t.Fatalf("add: %v", err)
	}
	if _, err := brl.Sub(usd); !errors.Is(err, ErrCurrencyMismatch) {
		t.Fatalf("sub: %v", err)
	}
	if _, err := brl.Cmp(usd); !errors.Is(err, ErrCurrencyMismatch) {
		t.Fatalf("cmp: %v", err)
	}
	if brl.Equal(usd) {
		t.Fatal("different currencies must not be equal")
	}
}

func TestCmp(t *testing.T) {
	a, b := MustNew(1, BRL), MustNew(2, BRL)
	for _, tc := range []struct {
		x, y Money
		want int
	}{{a, b, -1}, {b, a, 1}, {a, a, 0}} {
		got, err := tc.x.Cmp(tc.y)
		if err != nil || got != tc.want {
			t.Fatalf("cmp(%v,%v)=%d,%v want %d", tc.x, tc.y, got, err, tc.want)
		}
	}
}

func TestJSONSerialization(t *testing.T) {
	b, err := json.Marshal(MustNew(2500, BRL))
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != `{"amount":"25.00","currency":"BRL"}` {
		t.Fatalf("got %s", b)
	}
	if MustNew(5, BRL).Amount() != "0.05" {
		t.Fatal("small amount formatting")
	}
}
