package metering

import (
	"fmt"
	"math/rand"
	"path/filepath"
	"testing"
	"time"
)

var t0 = time.Date(2026, 11, 5, 10, 0, 0, 0, time.UTC) // aligned to a 10-minute interval

func gauge(meter, size string, v int64) Reading {
	return Reading{Meter: meter, Kind: Gauge, Labels: map[string]string{"size": size}, Unit: "broker-seconds", Value: v}
}

func sum(rs []Record, meter string) (q int64) {
	for _, r := range rs {
		if r.Meter == meter {
			q += r.Quantity
		}
	}
	return
}

func TestGaugeSplitsExactlyAtIntervalBoundaries(t *testing.T) {
	m := New(Config{Cell: "c", Interval: 10 * time.Minute})
	var all []Record
	// 3 brokers from 10:00, 6 from 10:07:30, sampled every 90 s until 10:30
	for at := t0; !at.After(t0.Add(30 * time.Minute)); at = at.Add(90 * time.Second) {
		n := int64(3)
		if !at.Before(t0.Add(450 * time.Second)) {
			n = 6
		}
		all = append(all, m.Observe("k", at, []Reading{gauge("broker_hours", "s", n)})...)
	}
	// The sample at 10:07:30 reads 6, but what ran during [10:06:00, 10:07:30) was the previous reading, 3.
	// Truth, second by second: readings change at sample instants; value over [t_i, t_{i+1}) is reading_i.
	truth := map[int64]int64{}
	for s := int64(0); s < 1800; s++ {
		at := t0.Add(time.Duration(s) * time.Second)
		last := t0
		for x := t0; !x.After(at); x = x.Add(90 * time.Second) {
			last = x
		}
		n := int64(3)
		if !last.Before(t0.Add(450 * time.Second)) {
			n = 6
		}
		truth[s/600] += n
	}
	got := map[int64]int64{}
	for _, r := range all {
		got[r.Start.Sub(t0).Milliseconds()/600000] += r.Quantity
	}
	for i := int64(0); i < 3; i++ {
		if got[i] != truth[i] {
			t.Errorf("interval %d: got %d, truth %d", i, got[i], truth[i])
		}
	}
	if len(all) != 3 { // the sample at 10:30:00 closes [10:20,10:30)
		t.Errorf("closed records = %d, want 3", len(all))
	}
}

func TestRecordIDsAreStableAndIgnoreQuantity(t *testing.T) {
	a := RecordID("c", "k", "broker_hours", map[string]string{"size": "s", "a": "b"}, t0, t0.Add(time.Hour), SchemaVersion)
	b := RecordID("c", "k", "broker_hours", map[string]string{"a": "b", "size": "s"}, t0, t0.Add(time.Hour), SchemaVersion)
	if a != b {
		t.Fatal("label order changes the id")
	}
	if a == RecordID("c", "k", "broker_hours", map[string]string{"size": "m"}, t0, t0.Add(time.Hour), SchemaVersion) ||
		a == RecordID("c", "k", "broker_hours", map[string]string{"size": "s", "a": "b"}, t0, t0.Add(time.Hour), SchemaVersion+1) {
		t.Fatal("different content, same id")
	}
	r := Record{Cell: "c", Cluster: "k", Meter: "m", Start: t0, End: t0.Add(time.Hour), Schema: 1, Quantity: 5}
	r.ID = RecordID(r.Cell, r.Cluster, r.Meter, r.Labels, r.Start, r.End, r.Schema)
	r.Quantity = 6
	if !r.Verify() {
		t.Fatal("quantity must not be part of the identity")
	}
	r.Meter = "other"
	if r.Verify() {
		t.Fatal("tampered record verifies")
	}
}

func TestCountersAndResets(t *testing.T) {
	m := New(Config{Cell: "c", Interval: 10 * time.Minute})
	rd := func(v int64) []Reading {
		return []Reading{{Meter: "data_in_bytes", Kind: Counter, Unit: "bytes", Value: v}}
	}
	var all []Record
	vals := []int64{1000, 1500, 2600, 100 /* reset */, 400, 400}
	for i, v := range vals {
		all = append(all, m.Observe("k", t0.Add(time.Duration(i)*5*time.Minute), rd(v))...)
	}
	all = append(all, m.Close("k", t0.Add(25*time.Minute))...)
	// readings at 0,5,10,15,20,25 min. Increases: 500@5 (iv0), 1100@10 (iv1), 100@15 (reset: new reading) (iv1),
	// 300@20 (iv2), 0@25 (iv2). The first reading is a baseline.
	want := map[int64]int64{0: 500, 1: 1200, 2: 300}
	got := map[int64]int64{}
	for _, r := range all {
		got[r.Start.Sub(t0).Milliseconds()/600000] += r.Quantity
	}
	for i, w := range want {
		if got[i] != w {
			t.Errorf("interval %d: %d, want %d", i, got[i], w)
		}
	}
}

func TestGapIsCarriedForwardOnlyUpToMaxGap(t *testing.T) {
	m := New(Config{Cell: "c", Interval: time.Hour, MaxGap: 5 * time.Minute})
	m.Observe("k", t0, []Reading{gauge("b", "s", 3)})
	m.Observe("k", t0.Add(30*time.Minute), []Reading{gauge("b", "s", 3)}) // 30 min without samples
	rs := m.Close("k", t0.Add(30*time.Minute))
	if q := sum(rs, "b"); q != 3*300 {
		t.Errorf("billed %d broker-seconds, want %d (5 minutes carried, 25 unknown)", q, 3*300)
	}
}

func TestRestartLosesNothing(t *testing.T) {
	// Run the same timeline once without restarts and once restarting the meter (through its persisted
	// state) before every sample: the records must be identical.
	run := func(restart bool) []Record {
		m := New(Config{Cell: "c", Interval: 10 * time.Minute, MaxGap: time.Hour})
		var all []Record
		rnd := rand.New(rand.NewSource(7))
		for i := 0; i < 40; i++ {
			if restart {
				b, err := m.MarshalState()
				if err != nil {
					t.Fatal(err)
				}
				m = New(Config{Cell: "c", Interval: 10 * time.Minute, MaxGap: time.Hour})
				if err := m.UnmarshalState(b); err != nil {
					t.Fatal(err)
				}
			}
			at := t0.Add(time.Duration(i) * 70 * time.Second)
			all = append(all, m.Observe("k", at, []Reading{gauge("b", "s", int64(rnd.Intn(7))),
				{Meter: "bytes", Kind: Counter, Unit: "bytes", Value: int64(i * 1000)}})...)
		}
		return append(all, m.Close("k", t0.Add(40*70*time.Second))...)
	}
	a, b := run(false), run(true)
	if len(a) != len(b) {
		t.Fatalf("%d vs %d records", len(a), len(b))
	}
	for i := range a {
		if a[i].ID != b[i].ID || a[i].Quantity != b[i].Quantity {
			t.Errorf("record %d differs: %+v vs %+v", i, a[i], b[i])
		}
	}
}

func TestRepeatedAndBackwardsTicksAreIgnored(t *testing.T) {
	m := New(Config{Cell: "c", Interval: 10 * time.Minute})
	m.Observe("k", t0, []Reading{gauge("b", "s", 3)})
	if rs := m.Observe("k", t0, []Reading{gauge("b", "s", 99)}); rs != nil {
		t.Fatal("a repeated tick produced records")
	}
	if rs := m.Observe("k", t0.Add(-time.Minute), []Reading{gauge("b", "s", 99)}); rs != nil {
		t.Fatal("a tick from the past produced records")
	}
	rs := m.Close("k", t0.Add(60*time.Second))
	if q := sum(rs, "b"); q != 180 {
		t.Errorf("quantity %d, want 180", q)
	}
}

func TestSpoolSurvivesRestartAndAcksOnlyWhatWasConfirmed(t *testing.T) {
	path := filepath.Join(t.TempDir(), "spool.jsonl")
	s, err := OpenSpool(path)
	if err != nil {
		t.Fatal(err)
	}
	var rs []Record
	for i := 0; i < 5; i++ {
		r := Record{Cell: "c", Cluster: "k", Meter: "m", Start: t0.Add(time.Duration(i) * time.Hour), End: t0.Add(time.Duration(i+1) * time.Hour), Schema: 1, Quantity: int64(i)}
		r.ID = RecordID(r.Cell, r.Cluster, r.Meter, nil, r.Start, r.End, r.Schema)
		rs = append(rs, r)
	}
	if err := s.Append(rs); err != nil || s.Append(rs) != nil {
		t.Fatal(err)
	}
	if s.Len() != 5 {
		t.Fatalf("appending twice duplicated: %d", s.Len())
	}
	s2, _ := OpenSpool(path) // restart
	if s2.Len() != 5 {
		t.Fatalf("after restart: %d", s2.Len())
	}
	if err := s2.Ack([]string{rs[0].ID, rs[3].ID, "unknown"}); err != nil {
		t.Fatal(err)
	}
	s3, _ := OpenSpool(path)
	if s3.Len() != 3 {
		t.Fatalf("after ack: %d", s3.Len())
	}
	p := s3.Pending(2)
	if len(p) != 2 || !p[0].Start.Before(p[1].Start) {
		t.Fatalf("pending order: %+v", p)
	}
	_ = fmt.Sprint()
}
