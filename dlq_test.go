package main

import "testing"

func TestDecision(t *testing.T) {
	tests := []struct {
		name  string
		event PaymentEvent
		want  string
	}{
		{"high value failure", PaymentEvent{ID: "p1", Amount: 150000, Status: "failed"}, "hold_and_notify"},
		{"ordinary failure", PaymentEvent{ID: "p2", Amount: 5000, Status: "failed"}, "dead_letter"},
		{"success", PaymentEvent{ID: "p3", Amount: 5000, Status: "paid"}, "ack"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := decide(tt.event).Action; got != tt.want {
				t.Fatalf("got %q, want %q", got, tt.want)
			}
		})
	}
}
