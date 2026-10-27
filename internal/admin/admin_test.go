package admin

import (
	"testing"

	"edgemesh/internal/anomaly"
)

func TestAlertLogRing(t *testing.T) {
	l := NewAlertLog(3)
	if len(l.Recent()) != 0 {
		t.Fatal("should start empty")
	}
	for i := 1; i <= 5; i++ {
		l.Add(anomaly.Alert{Value: float64(i)})
	}
	got := l.Recent()
	if len(got) != 3 || got[0].Value != 5 || got[2].Value != 3 {
		t.Fatalf("%+v", got)
	}
}
