// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package events_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"testing"

	"github.com/cratis/chronicle.go/events"
)

func TestSequenceNumberConstantsAndPredicates(t *testing.T) {
	// An alias retains the public methods and accepts the typed constants.
	type position = events.SequenceNumber
	tests := []struct {
		name                             string
		value                            position
		encoded                          string
		actual, unavailable, beforeFirst bool
	}{
		{"zero", position(0), "0", true, false, false},
		{"first", events.First, "0", true, false, false},
		{"one", position(1), "1", true, false, false},
		{"last actual", events.BeforeFirst - 1, "18446744073709551612", true, false, false},
		{"before first", events.BeforeFirst, "18446744073709551613", false, false, true},
		{"max", events.Max, "18446744073709551614", false, false, false},
		{"unavailable", events.Unavailable, "18446744073709551615", false, true, false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.value.IsActualValue() != test.actual || test.value.IsUnavailable() != test.unavailable || test.value.IsBeforeFirst() != test.beforeFirst {
				t.Fatalf("predicates for %d = %v/%v/%v", test.value, test.value.IsActualValue(), test.value.IsUnavailable(), test.value.IsBeforeFirst())
			}
			if test.value.IsActualValue() != (test.value != events.Unavailable && test.value != events.Max && test.value != events.BeforeFirst) {
				t.Fatal("actual-value predicate is not the inverse of reserved membership")
			}
			encoded, err := json.Marshal(test.value)
			if err != nil || string(encoded) != test.encoded {
				t.Fatalf("JSON = %s, %v; want %s", encoded, err, test.encoded)
			}
			var decoded position
			if err := json.Unmarshal(encoded, &decoded); err != nil || decoded != test.value {
				t.Fatalf("JSON round trip = %d, %v; want %d", decoded, err, test.value)
			}
		})
	}
}

func TestSequenceNumberSentinelReceiversArePreserved(t *testing.T) {
	for _, sentinel := range []events.SequenceNumber{events.Unavailable, events.Max, events.BeforeFirst} {
		t.Run(fmt.Sprint(sentinel), func(t *testing.T) {
			if next, err := sentinel.Next(); err != nil || next != sentinel {
				t.Fatalf("Next = %d, %v; want %d, nil", next, err, sentinel)
			}
			for _, delta := range []uint64{0, 1, uint64(events.BeforeFirst), uint64(events.Max), math.MaxUint64} {
				t.Run(fmt.Sprint(delta), func(t *testing.T) {
					for name, operation := range map[string]func(uint64) (events.SequenceNumber, error){"add": sentinel.Add, "subtract": sentinel.Subtract} {
						if got, err := operation(delta); err != nil || got != sentinel {
							t.Errorf("%s = %d, %v; want %d, nil", name, got, err, sentinel)
						}
					}
				})
			}
		})
	}
}

func TestSequenceNumberCheckedArithmetic(t *testing.T) {
	last := events.BeforeFirst - 1
	tests := []struct {
		name                    string
		value                   events.SequenceNumber
		delta                   uint64
		add, subtract           events.SequenceNumber
		addFails, subtractFails bool
	}{
		{"zero unchanged", 0, 0, 0, 0, false, false},
		{"first one", events.First, 1, 1, events.Unavailable, false, true},
		{"first maximum amount", events.First, math.MaxUint64, events.Unavailable, events.Unavailable, true, true},
		{"one to first", 1, 1, 2, events.First, false, false},
		{"first to last actual", events.First, uint64(last), last, events.Unavailable, false, true},
		{"last unchanged", last, 0, last, last, false, false},
		{"last crosses before first", last, 1, events.Unavailable, last - 1, true, false},
		{"last crosses max", last, 2, events.Unavailable, last - 2, true, false},
		{"last crosses unavailable", last, 3, events.Unavailable, last - 3, true, false},
		{"last wraps", last, 4, events.Unavailable, last - 4, true, false},
		{"last maximum amount", last, math.MaxUint64, events.Unavailable, events.Unavailable, true, true},
		{"last to first", last, uint64(last), events.Unavailable, events.First, true, false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			for _, operation := range []struct {
				name  string
				call  func(uint64) (events.SequenceNumber, error)
				want  events.SequenceNumber
				fails bool
			}{
				{"add", test.value.Add, test.add, test.addFails},
				{"subtract", test.value.Subtract, test.subtract, test.subtractFails},
			} {
				got, err := operation.call(test.delta)
				if got != operation.want || (operation.fails && !errors.Is(err, events.ErrSequenceNumberRange)) || (!operation.fails && err != nil) {
					t.Errorf("%s = %d, %v; want %d, range error=%v", operation.name, got, err, operation.want, operation.fails)
				}
			}
		})
	}
}

func TestSequenceNumberNextActualBoundaries(t *testing.T) {
	for _, value := range []events.SequenceNumber{events.First, 1, events.BeforeFirst - 2, events.BeforeFirst - 1} {
		t.Run(fmt.Sprint(value), func(t *testing.T) {
			got, err := value.Next()
			if value == events.BeforeFirst-1 {
				if got != events.Unavailable || !errors.Is(err, events.ErrSequenceNumberRange) {
					t.Fatalf("exhausted Next = %d, %v", got, err)
				}
			} else if got != value+1 || err != nil {
				t.Fatalf("Next = %d, %v; want %d, nil", got, err, value+1)
			}
		})
	}
}

func TestSequenceNumberArithmeticRoundTrips(t *testing.T) {
	last := events.BeforeFirst - 1
	for _, value := range []events.SequenceNumber{events.First, 1, 2, 9, last - 2, last - 1, last} {
		for _, delta := range []uint64{0, 1, 2, 9, uint64(last - 1), uint64(last), uint64(events.BeforeFirst), math.MaxUint64} {
			t.Run(fmt.Sprintf("%d/%d", value, delta), func(t *testing.T) {
				if delta <= uint64(last-value) {
					added, err := value.Add(delta)
					if err != nil || !added.IsActualValue() {
						t.Fatalf("representable Add = %d, %v", added, err)
					}
					back, err := added.Subtract(delta)
					if err != nil || back != value {
						t.Fatalf("Add/Subtract round trip = %d, %v; want %d", back, err, value)
					}
				}
				if delta <= uint64(value) {
					subtracted, err := value.Subtract(delta)
					if err != nil || !subtracted.IsActualValue() {
						t.Fatalf("representable Subtract = %d, %v", subtracted, err)
					}
					back, err := subtracted.Add(delta)
					if err != nil || back != value {
						t.Fatalf("Subtract/Add round trip = %d, %v; want %d", back, err, value)
					}
				}
			})
		}
	}
}
