package schema

import (
	"encoding/json"
	"net/url"
	"testing"
)

func Test_VolumePatch(t *testing.T) {
	// null removes an option, a list replaces its values, where an empty
	// list has none, and a string, number or boolean is the one value
	var patch VolumePatch
	if err := json.Unmarshal([]byte(`{"a": null, "b": ["x", "y"], "c": [], "d": "x", "e": true, "f": 1.5, "g": [false, 2]}`), &patch); err != nil {
		t.Fatal(err)
	}
	values, err := patch.Values()
	if err != nil {
		t.Fatal(err)
	}
	want := url.Values{"a": {}, "b": {"x", "y"}, "c": {""}, "d": {"x"}, "e": {"true"}, "f": {"1.5"}, "g": {"false", "2"}}
	if len(values) != len(want) {
		t.Fatalf("got %v, want %v", values, want)
	}
	for key, want := range want {
		got, exists := values[key]
		if !exists || len(got) != len(want) {
			t.Errorf("%s: got %q, want %q", key, got, want)
			continue
		}
		for i := range want {
			if got[i] != want[i] {
				t.Errorf("%s: got %q, want %q", key, got, want)
			}
		}
	}

	// An object, or a list of them, or of lists, isn't an option
	for _, body := range []string{`{"a": {}}`, `{"a": [{}]}`, `{"a": [[]]}`, `{"a": [null]}`, `{"": true}`} {
		var patch VolumePatch
		if err := json.Unmarshal([]byte(body), &patch); err != nil {
			t.Fatal(err)
		} else if _, err := patch.Values(); err == nil {
			t.Errorf("%s: no error", body)
		}
	}
}

func Test_FormatSize(t *testing.T) {
	for size, want := range map[int64]string{
		0:          "0 B",
		1023:       "1023 B",
		1024:       "1.0 KiB",
		1536:       "1.5 KiB",
		5 << 20:    "5.0 MiB",
		3 << 30:    "3.0 GiB",
		2 << 40:    "2.0 TiB",
		2048 << 40: "2048.0 TiB",
	} {
		if got := FormatSize(size); got != want {
			t.Errorf("FormatSize(%d) = %q, want %q", size, got, want)
		}
	}
}
