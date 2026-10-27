package anomaly

import (
	"math/rand/v2"
	"testing"

	"edgemesh/internal/telemetry"
)

const sec = 1_000_000

func feed(d *Detector, dev, metric string, i int, v float64) (telemetry.Reading, Alert, bool) {
	r := telemetry.Reading{DeviceID: dev, Metric: metric, Value: v, TS: int64(i) * sec, RecvTS: int64(i) * sec}
	a, fire := d.Observe(&r)
	return r, a, fire
}

func warm(d *Detector, dev, metric string, n int, base, noise float64) int {
	rng := rand.New(rand.NewPCG(1, 2))
	for i := range n {
		feed(d, dev, metric, i, base+(rng.Float64()-0.5)*noise)
	}
	return n
}

func TestNormalTrafficStaysQuiet(t *testing.T) {
	d := New(Config{})
	rng := rand.New(rand.NewPCG(7, 7))
	for i := range 2000 {
		_, _, fire := feed(d, "d1", "engine_temp", i, 80+(rng.Float64()-0.5)*2)
		if fire {
			t.Fatalf("false positive at %d", i)
		}
	}
}

func TestHardThreshold(t *testing.T) {
	d := New(Config{})
	r, a, fire := feed(d, "d1", "engine_temp", 0, 200)
	if !fire || a.Kind != KindThreshold || !r.Anomalous() {
		t.Fatalf("fire=%v alert=%+v", fire, a)
	}
	if a.Score != 70 {
		t.Fatalf("score=%v", a.Score)
	}
}

func TestSuddenSpikeByRate(t *testing.T) {
	d := New(Config{})
	i := warm(d, "d1", "engine_temp", 40, 80, 1)
	_, a, fire := feed(d, "d1", "engine_temp", i, 110) // +30C in 1s, under the 130 limit
	if !fire || a.Kind != KindRate {
		t.Fatalf("fire=%v alert=%+v", fire, a)
	}
}

func TestZScoreWithoutRateRule(t *testing.T) {
	d := New(Config{})
	i := warm(d, "d1", "vibration", 60, 2, 0.4)
	_, a, fire := feed(d, "d1", "vibration", i, 6)
	if !fire || a.Kind != KindZScore {
		t.Fatalf("fire=%v alert=%+v", fire, a)
	}
}

func TestWarmupDoesNotZScore(t *testing.T) {
	d := New(Config{})
	warm(d, "d1", "vibration", 5, 2, 0.1)
	if _, _, fire := feed(d, "d1", "vibration", 5, 9); fire {
		t.Fatal("z-score fired before warmup")
	}
}

func TestCooldownSuppressesAlertButNotFlag(t *testing.T) {
	d := New(Config{})
	_, _, fire := feed(d, "d1", "engine_temp", 0, 200)
	if !fire {
		t.Fatal("first should fire")
	}
	r, _, fire := feed(d, "d1", "engine_temp", 1, 201)
	if fire {
		t.Fatal("second inside cooldown should not fire")
	}
	if !r.Anomalous() {
		t.Fatal("still must be flagged for the cloud")
	}
	if _, _, fire := feed(d, "d1", "engine_temp", 10, 202); !fire {
		t.Fatal("should fire again after cooldown")
	}
}

func TestSustainedShiftBecomesNewBaseline(t *testing.T) {
	d := New(Config{})
	i := warm(d, "d1", "vibration", 60, 2, 0.2)
	fired := 0
	for j := range 200 {
		if _, _, fire := feed(d, "d1", "vibration", i+j, 8+float64(j%2)*0.1); fire {
			fired++
		}
	}
	if fired == 0 || fired > 5 {
		t.Fatalf("fired %d times, expected a handful then silence", fired)
	}
}

func TestDevicesAreIndependent(t *testing.T) {
	d := New(Config{})
	warm(d, "a", "load", 60, 2, 0.2)
	warm(d, "b", "load", 60, 50, 0.2)
	if _, _, fire := feed(d, "b", "load", 61, 50.1); fire {
		t.Fatal("b is normal at 50")
	}
}

func TestSweep(t *testing.T) {
	d := New(Config{})
	feed(d, "old", "m", 1, 1)
	feed(d, "new", "m", 100, 1)
	if n := d.Sweep(50 * sec); n != 1 || d.Series() != 1 {
		t.Fatalf("removed=%d series=%d", n, d.Series())
	}
}

func BenchmarkObserve(b *testing.B) {
	d := New(Config{})
	r := telemetry.Reading{DeviceID: "dev-1", Metric: "engine_temp", Value: 80}
	for i := 0; i < b.N; i++ {
		r.TS = int64(i) * sec
		d.Observe(&r)
	}
}
