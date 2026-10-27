package config

import "testing"

func TestDefaultsValid(t *testing.T) {
	if _, err := Parse(nil); err != nil {
		t.Fatal(err)
	}
}

func TestRejectsBadValues(t *testing.T) {
	for _, args := range [][]string{
		{"-sync=sometimes"},
		{"-min-batch=100", "-max-batch=10"},
		{"-suspect-after=30s", "-offline-after=10s"},
		{"-segment-mb=64", "-wal-max-mb=100"},
		{"-workers=0"},
	} {
		if _, err := Parse(args); err == nil {
			t.Errorf("%v accepted", args)
		}
	}
}
