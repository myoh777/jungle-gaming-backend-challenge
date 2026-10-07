// Package money implements an exact, immutable monetary value object.
//
// Amounts are stored as int64 minor units (cents). Every supported currency
// uses a fixed scale of two decimal places. The representable range is
// [-MaxMinorUnits, +MaxMinorUnits]; math.MinInt64 is excluded on purpose so
// negation can never overflow. Floating point is never used.
package money

import (
	"errors"
	"fmt"
	"math"
	"strings"
)

// Scale is the fixed number of decimal places for all supported currencies.
const Scale = 2

// MaxMinorUnits is the largest absolute amount, in minor units, a Money can hold.
const MaxMinorUnits int64 = math.MaxInt64

var (
	ErrInvalidAmount       = errors.New("money: invalid amount")
	ErrNegativeAmount      = errors.New("money: negative amount not allowed")
	ErrScaleExceeded       = errors.New("money: amount has more than 2 decimal places")
	ErrOverflow            = errors.New("money: amount overflow")
	ErrUnsupportedCurrency = errors.New("money: unsupported currency")
	ErrCurrencyMismatch    = errors.New("money: currency mismatch")
)

// Currency is an ISO 4217 alphabetic code.
type Currency string

const (
	BRL Currency = "BRL"
	USD Currency = "USD"
	EUR Currency = "EUR"
)

// supported lists the ISO 4217 currencies accepted by the service. All of them
// have exactly two minor-unit digits, which is what makes the fixed Scale valid.
var supported = map[Currency]struct{}{BRL: {}, USD: {}, EUR: {}}

// ParseCurrency validates an ISO 4217 code. It is case-sensitive: "brl" is rejected.
func ParseCurrency(code string) (Currency, error) {
	c := Currency(code)
	if _, ok := supported[c]; !ok {
		return "", fmt.Errorf("%w: %q", ErrUnsupportedCurrency, code)
	}
	return c, nil
}

// Money is an immutable amount in minor units plus its currency.
// The zero value is not a valid Money; use Zero, New or a Parse function.
type Money struct {
	minor    int64
	currency Currency
}

// Zero returns zero in the given currency.
func Zero(c Currency) (Money, error) {
	return New(0, c)
}

// New builds a Money from minor units.
func New(minor int64, c Currency) (Money, error) {
	if _, ok := supported[c]; !ok {
		return Money{}, fmt.Errorf("%w: %q", ErrUnsupportedCurrency, c)
	}
	if minor == math.MinInt64 {
		return Money{}, ErrOverflow
	}
	return Money{minor: minor, currency: c}, nil
}

// MustNew is a helper for constants in tests and internal code paths whose
// inputs are known to be valid. It must not be used with external input.
func MustNew(minor int64, c Currency) Money {
	m, err := New(minor, c)
	if err != nil {
		panic(err)
	}
	return m
}

// ParseNonNegative parses an external decimal amount ("25.00", "25.5", "25").
// It rejects empty strings, signs, NaN, Infinity, exponents, whitespace,
// leading zeros such as "007", more than two decimals, and overflow.
// Inputs are never rounded.
func ParseNonNegative(amount string, currency string) (Money, error) {
	if strings.HasPrefix(amount, "-") {
		return Money{}, ErrNegativeAmount
	}
	return Parse(amount, currency)
}

// Parse parses a decimal amount that may carry a leading '-'. It is meant for
// internal values; external financial inputs must use ParseNonNegative.
func Parse(amount string, currency string) (Money, error) {
	c, err := ParseCurrency(currency)
	if err != nil {
		return Money{}, err
	}
	minor, err := parseMinor(amount)
	if err != nil {
		return Money{}, err
	}
	return Money{minor: minor, currency: c}, nil
}

func parseMinor(s string) (int64, error) {
	if s == "" {
		return 0, fmt.Errorf("%w: empty", ErrInvalidAmount)
	}
	negative := false
	if s[0] == '-' {
		negative = true
		s = s[1:]
	}
	intPart, fracPart, hasDot := strings.Cut(s, ".")
	if intPart == "" || (hasDot && fracPart == "") {
		return 0, fmt.Errorf("%w: %q", ErrInvalidAmount, s)
	}
	if !allDigits(intPart) || !allDigits(fracPart) {
		// Covers NaN, Infinity, exponents ("1e3"), '+', spaces and separators.
		return 0, fmt.Errorf("%w: %q", ErrInvalidAmount, s)
	}
	if len(intPart) > 1 && intPart[0] == '0' {
		return 0, fmt.Errorf("%w: leading zeros in %q", ErrInvalidAmount, s)
	}
	if len(fracPart) > Scale {
		return 0, ErrScaleExceeded
	}
	for len(fracPart) < Scale {
		fracPart += "0"
	}

	var minor int64
	for _, r := range intPart + fracPart {
		d := int64(r - '0')
		if minor > (MaxMinorUnits-d)/10 {
			return 0, ErrOverflow
		}
		minor = minor*10 + d
	}
	if negative {
		minor = -minor
	}
	return minor, nil
}

func allDigits(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

// MinorUnits returns the amount in minor units (cents).
func (m Money) MinorUnits() int64 { return m.minor }

// Currency returns the ISO 4217 code.
func (m Money) Currency() Currency { return m.currency }

func (m Money) IsZero() bool     { return m.minor == 0 }
func (m Money) IsPositive() bool { return m.minor > 0 }
func (m Money) IsNegative() bool { return m.minor < 0 }

// Add returns m + o. Currencies must match and the result must not overflow.
func (m Money) Add(o Money) (Money, error) {
	if err := m.sameCurrency(o); err != nil {
		return Money{}, err
	}
	if (o.minor > 0 && m.minor > MaxMinorUnits-o.minor) ||
		(o.minor < 0 && m.minor < -MaxMinorUnits-o.minor) {
		return Money{}, ErrOverflow
	}
	return Money{minor: m.minor + o.minor, currency: m.currency}, nil
}

// Sub returns m - o. Currencies must match and the result must not overflow.
func (m Money) Sub(o Money) (Money, error) {
	neg, err := o.Negate()
	if err != nil {
		return Money{}, err
	}
	return m.Add(neg)
}

// Negate returns -m. It cannot overflow because math.MinInt64 is never stored,
// but the error is kept so callers do not depend on that detail.
func (m Money) Negate() (Money, error) {
	if m.minor == math.MinInt64 {
		return Money{}, ErrOverflow
	}
	return Money{minor: -m.minor, currency: m.currency}, nil
}

// Cmp returns -1, 0 or +1. Currencies must match.
func (m Money) Cmp(o Money) (int, error) {
	if err := m.sameCurrency(o); err != nil {
		return 0, err
	}
	switch {
	case m.minor < o.minor:
		return -1, nil
	case m.minor > o.minor:
		return 1, nil
	default:
		return 0, nil
	}
}

// Equal reports whether amount and currency are identical.
func (m Money) Equal(o Money) bool { return m == o }

func (m Money) sameCurrency(o Money) error {
	if m.currency != o.currency {
		return fmt.Errorf("%w: %s vs %s", ErrCurrencyMismatch, m.currency, o.currency)
	}
	return nil
}

// Amount renders the decimal amount with exactly two decimals, e.g. "25.00" or "-0.05".
func (m Money) Amount() string {
	v := m.minor
	sign := ""
	if v < 0 {
		sign = "-"
		v = -v
	}
	return fmt.Sprintf("%s%d.%02d", sign, v/100, v%100)
}

// String renders "25.00 BRL".
func (m Money) String() string { return m.Amount() + " " + string(m.currency) }

// JSON is the external wire representation: {"amount":"25.00","currency":"BRL"}.
type JSON struct {
	Amount   string `json:"amount"`
	Currency string `json:"currency"`
}

// ToJSON converts to the wire representation.
func (m Money) ToJSON() JSON { return JSON{Amount: m.Amount(), Currency: string(m.currency)} }

// MarshalJSON serializes Money as {"amount":"25.00","currency":"BRL"}.
func (m Money) MarshalJSON() ([]byte, error) {
	return []byte(`{"amount":"` + m.Amount() + `","currency":"` + string(m.currency) + `"}`), nil
}
