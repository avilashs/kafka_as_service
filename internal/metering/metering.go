// Package metering turns samples of a running resource into billing records that are exact and idempotent
// (ADR-0017, design §17). The accounting is integer arithmetic over whole seconds, split exactly at interval
// boundaries, so a total can be reconciled against an independent timeline to the unit. Nothing here knows
// about Kubernetes or Kafka.
//
// Meters come in two kinds:
//
//   - gauges (running broker pods, provisioned GiB): the quantity is value × seconds, in "broker-seconds" or
//     "GiB-seconds"; hours are derived at invoicing;
//   - counters (bytes in and out): the quantity is the increase of the counter, attributed to the interval
//     in which it was read; a counter that goes down has reset, and the new reading is the increase.
//
// A record is identified by sha256 over everything that identifies what it measures, never over the quantity;
// the control plane inserts with ON CONFLICT DO NOTHING and flags a different quantity for the same ID.
package metering

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"
)

// SchemaVersion is part of every record ID. Changing what a meter means changes the version.
const SchemaVersion = 1

// Record is one closed interval of one meter of one resource.
type Record struct {
	ID       string            `json:"id"`
	Cell     string            `json:"cell"`
	Cluster  string            `json:"cluster"`
	Meter    string            `json:"meter"`
	Labels   map[string]string `json:"labels,omitempty"`
	Start    time.Time         `json:"start"`
	End      time.Time         `json:"end"`
	Quantity int64             `json:"quantity"`
	Unit     string            `json:"unit"`
	Schema   int               `json:"schema"`
}

// RecordID computes the deterministic identifier of a record.
func RecordID(cell, cluster, meter string, labels map[string]string, start, end time.Time, schema int) string {
	h := sha256.New()
	fmt.Fprintf(h, "%s\x00%s\x00%s\x00%s\x00%d\x00%d\x00%d", cell, cluster, meter, LabelString(labels), start.Unix(), end.Unix(), schema)
	return hex.EncodeToString(h.Sum(nil))
}

// LabelString is the canonical form of a label set.
func LabelString(l map[string]string) string {
	keys := make([]string, 0, len(l))
	for k := range l {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	for i, k := range keys {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(k + "=" + l[k])
	}
	return b.String()
}

// Verify reports whether the record's ID matches its content.
func (r Record) Verify() bool {
	return r.ID == RecordID(r.Cell, r.Cluster, r.Meter, r.Labels, r.Start, r.End, r.Schema)
}

// Kind of a meter.
type Kind string

const (
	Gauge   Kind = "gauge"
	Counter Kind = "counter"
)

// Reading is one value of one meter at one moment.
type Reading struct {
	Meter  string            `json:"meter"`
	Kind   Kind              `json:"kind"`
	Labels map[string]string `json:"labels,omitempty"`
	Unit   string            `json:"unit"`
	Value  int64             `json:"value"`
}

func (r Reading) key() string {
	return r.Meter + "\x00" + string(r.Kind) + "\x00" + r.Unit + "\x00" + LabelString(r.Labels)
}

// Config shapes the accounting.
type Config struct {
	Cell string
	// Interval is the length of a billing interval (an hour in production). Intervals are aligned to the Unix epoch.
	Interval time.Duration
	// MaxGap bounds how long the last state is carried forward over missed samples (agent down, cell
	// unreachable). Beyond it the time is not billed: unknown is not billable.
	MaxGap time.Duration
}

func (c Config) interval() int64 {
	if c.Interval <= 0 {
		return 3600
	}
	return int64(c.Interval / time.Second)
}

// Meter accumulates readings per resource. It is not safe for concurrent use.
type Meter struct {
	cfg Config
	st  State
}

// State is everything the meter must remember to survive a restart.
type State struct {
	Clusters map[string]*clusterState `json:"clusters"`
}

type clusterState struct {
	LastAt int64                     `json:"lastAt"`
	Last   map[string]Reading        `json:"last"`
	Open   map[int64]map[string]*acc `json:"open"` // interval start (Unix seconds) -> meter key -> accumulator
}

type acc struct {
	Reading Reading `json:"reading"`
	Qty     int64   `json:"qty"`
}

// New returns a meter.
func New(cfg Config) *Meter {
	return &Meter{cfg: cfg, st: State{Clusters: map[string]*clusterState{}}}
}

// MarshalState returns the persisted form of the meter. Write it (atomically) after every Observe: a
// restart then loses nothing that was already attributed.
func (m *Meter) MarshalState() ([]byte, error) { return json.Marshal(m.st) }

// UnmarshalState restores a persisted state.
func (m *Meter) UnmarshalState(b []byte) error {
	var s State
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	if s.Clusters == nil {
		s.Clusters = map[string]*clusterState{}
	}
	for _, c := range s.Clusters {
		if c.Last == nil {
			c.Last = map[string]Reading{}
		}
		if c.Open == nil {
			c.Open = map[int64]map[string]*acc{}
		}
	}
	m.st = s
	return nil
}

// Clusters lists the resources the meter holds state for.
func (m *Meter) Clusters() []string {
	out := make([]string, 0, len(m.st.Clusters))
	for id := range m.st.Clusters {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}

func (m *Meter) cluster(id string) *clusterState {
	c := m.st.Clusters[id]
	if c == nil {
		c = &clusterState{Last: map[string]Reading{}, Open: map[int64]map[string]*acc{}}
		m.st.Clusters[id] = c
	}
	return c
}

func (c *clusterState) add(start int64, r Reading, qty int64) {
	accs := c.Open[start]
	if accs == nil {
		accs = map[string]*acc{}
		c.Open[start] = accs
	}
	a := accs[r.key()]
	if a == nil {
		a = &acc{Reading: Reading{Meter: r.Meter, Kind: r.Kind, Labels: r.Labels, Unit: r.Unit}}
		accs[r.key()] = a
	}
	a.Qty += qty
}

// Observe records the readings taken at time at and returns the intervals that are now complete.
// Readings must be complete for the resource: a gauge that was present before and is absent now has
// dropped to zero (a size that no longer runs).
func (m *Meter) Observe(cluster string, at time.Time, readings []Reading) []Record {
	c := m.cluster(cluster)
	t := at.Unix()
	if c.LastAt != 0 && t <= c.LastAt {
		return nil // time did not advance (a repeated tick, or a clock step back): nothing to attribute
	}
	if c.LastAt != 0 {
		m.attribute(c, c.LastAt, t, readings)
	}
	c.LastAt = t
	c.Last = map[string]Reading{}
	for _, r := range readings {
		c.Last[r.key()] = r
	}
	return m.closed(cluster, c, t)
}

// attribute spreads the previous gauge values over [from, to) (bounded by MaxGap), splitting at interval
// boundaries, and attributes counter increases to the interval of to.
func (m *Meter) attribute(c *clusterState, from, to int64, now []Reading) {
	iv := m.cfg.interval()
	end := to
	if max := int64(m.cfg.MaxGap / time.Second); max > 0 && end-from > max {
		end = from + max
	}
	for _, prev := range c.Last {
		if prev.Kind != Gauge || prev.Value == 0 {
			continue
		}
		for s := from; s < end; {
			start := s - mod(s, iv)
			stop := min(start+iv, end)
			c.add(start, prev, prev.Value*(stop-s))
			s = stop
		}
	}
	start := to - mod(to, iv)
	for _, cur := range now {
		if cur.Kind != Counter {
			continue
		}
		delta := cur.Value // a series that appears while the resource is tracked starts at zero (a new pod)
		if prev, ok := c.Last[cur.key()]; ok && cur.Value >= prev.Value {
			delta = cur.Value - prev.Value
		} // else: the counter went down, it was reset, and the new reading is the increase
		c.add(start, cur, delta)
	}
	// make sure every meter that is present now has an (possibly zero) entry in the interval of `to`
	for _, cur := range now {
		c.add(start, cur, 0)
	}
}

func mod(a, b int64) int64 { return ((a % b) + b) % b }

// closed turns every interval that ended at or before t into records.
func (m *Meter) closed(cluster string, c *clusterState, t int64) []Record {
	iv := m.cfg.interval()
	var starts []int64
	for s := range c.Open {
		if s+iv <= t {
			starts = append(starts, s)
		}
	}
	sort.Slice(starts, func(i, j int) bool { return starts[i] < starts[j] })
	var out []Record
	for _, s := range starts {
		out = append(out, m.records(cluster, s, s+iv, c.Open[s])...)
		delete(c.Open, s)
	}
	return out
}

func (m *Meter) records(cluster string, start, end int64, accs map[string]*acc) []Record {
	keys := make([]string, 0, len(accs))
	for k := range accs {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var out []Record
	for _, k := range keys {
		a := accs[k]
		r := Record{Cell: m.cfg.Cell, Cluster: cluster, Meter: a.Reading.Meter, Labels: a.Reading.Labels,
			Start: time.Unix(start, 0).UTC(), End: time.Unix(end, 0).UTC(), Quantity: a.Qty, Unit: a.Reading.Unit, Schema: SchemaVersion}
		r.ID = RecordID(r.Cell, r.Cluster, r.Meter, r.Labels, r.Start, r.End, r.Schema)
		out = append(out, r)
	}
	return out
}

// Close ends the metering of a resource at time at (it was deleted): the last state is attributed up to
// at, and every open interval is closed, partial or not. The resource's state is forgotten.
func (m *Meter) Close(cluster string, at time.Time) []Record {
	c := m.st.Clusters[cluster]
	if c == nil {
		return nil
	}
	if t := at.Unix(); c.LastAt != 0 && t > c.LastAt {
		m.attribute(c, c.LastAt, t, nil)
	}
	var starts []int64
	for s := range c.Open {
		starts = append(starts, s)
	}
	sort.Slice(starts, func(i, j int) bool { return starts[i] < starts[j] })
	iv := m.cfg.interval()
	var out []Record
	for _, s := range starts {
		out = append(out, m.records(cluster, s, s+iv, c.Open[s])...)
	}
	delete(m.st.Clusters, cluster)
	return out
}
