package kafka

import (
	"sort"
	"strconv"
	"strings"

	"github.com/avilashs/kafka_as_service/internal/platform/engine"
)

// configRule bounds one customer-settable broker configuration key.
type configRule struct {
	kind     string // "int", "bool", "enum", "string"
	min, max int64
	enum     []string
}

// allowedConfig is the explicit allowlist of customer-settable broker configs
// (design §15.1). Anything not listed is rejected; nothing touching listeners,
// security, plugins, storage paths or the metadata quorum appears here.
var allowedConfig = map[string]configRule{
	"auto.create.topics.enable":           {kind: "bool"},
	"compression.type":                    {kind: "enum", enum: []string{"producer", "uncompressed", "gzip", "snappy", "lz4", "zstd"}},
	"default.replication.factor":          {kind: "int", min: 2, max: 3},
	"min.insync.replicas":                 {kind: "int", min: 1, max: 2},
	"num.partitions":                      {kind: "int", min: 1, max: 256},
	"log.retention.ms":                    {kind: "int", min: -1, max: 1 << 50},
	"log.retention.hours":                 {kind: "int", min: 1, max: 87600},
	"log.retention.bytes":                 {kind: "int", min: -1, max: 1 << 50},
	"log.segment.bytes":                   {kind: "int", min: 1 << 24, max: 1 << 30},
	"log.roll.ms":                         {kind: "int", min: 60_000, max: 1 << 40},
	"log.cleanup.policy":                  {kind: "enum", enum: []string{"delete", "compact", "compact,delete"}},
	"log.cleaner.delete.retention.ms":     {kind: "int", min: 0, max: 1 << 40},
	"log.message.timestamp.type":          {kind: "enum", enum: []string{"CreateTime", "LogAppendTime"}},
	"message.max.bytes":                   {kind: "int", min: 1024, max: 8 << 20},
	"replica.fetch.max.bytes":             {kind: "int", min: 1024, max: 8 << 20},
	"offsets.retention.minutes":           {kind: "int", min: 60, max: 1 << 30},
	"transaction.max.timeout.ms":          {kind: "int", min: 1000, max: 3_600_000},
	"transactional.id.expiration.ms":      {kind: "int", min: 60_000, max: 1 << 40},
	"unclean.leader.election.enable":      {kind: "bool"},
	"group.initial.rebalance.delay.ms":    {kind: "int", min: 0, max: 300_000},
	"delete.topic.enable":                 {kind: "bool"},
	"producer.id.expiration.ms":           {kind: "int", min: 60_000, max: 1 << 40},
	"log.message.timestamp.before.max.ms": {kind: "int", min: 0, max: 1 << 50},
	"log.message.timestamp.after.max.ms":  {kind: "int", min: 0, max: 1 << 50},
}

// platformConfig is always set by the platform and cannot be overridden.
var platformConfig = map[string]any{
	"default.replication.factor":               3,
	"min.insync.replicas":                      2,
	"offsets.topic.replication.factor":         3,
	"transaction.state.log.replication.factor": 3,
	"transaction.state.log.min.isr":            2,
	"replica.selector.class":                   "org.apache.kafka.common.replica.RackAwareReplicaSelector",
	"auto.create.topics.enable":                false,
}

// overridable lists platform defaults a customer may change within allowedConfig bounds.
var overridable = map[string]bool{
	"default.replication.factor": true,
	"min.insync.replicas":        true,
	"auto.create.topics.enable":  true,
}

// ValidateConfig checks customer configuration against the allowlist.
func ValidateConfig(cfg map[string]string) error {
	keys := make([]string, 0, len(cfg))
	for k := range cfg {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		v := cfg[k]
		rule, ok := allowedConfig[k]
		if !ok {
			return engine.Invalid("config %q is not customer-settable", k)
		}
		if _, isPlatform := platformConfig[k]; isPlatform && !overridable[k] {
			return engine.Invalid("config %q is managed by the platform", k)
		}
		switch rule.kind {
		case "int":
			n, err := strconv.ParseInt(v, 10, 64)
			if err != nil || n < rule.min || n > rule.max {
				return engine.Invalid("config %q must be an integer in [%d, %d]", k, rule.min, rule.max)
			}
		case "bool":
			if v != "true" && v != "false" {
				return engine.Invalid("config %q must be true or false", k)
			}
		case "enum":
			found := false
			for _, e := range rule.enum {
				if v == e {
					found = true
				}
			}
			if !found {
				return engine.Invalid("config %q must be one of %s", k, strings.Join(rule.enum, ", "))
			}
		}
	}
	if mir, ok := cfg["min.insync.replicas"]; ok {
		rf := "3"
		if v, ok := cfg["default.replication.factor"]; ok {
			rf = v
		}
		m, _ := strconv.Atoi(mir)
		r, _ := strconv.Atoi(rf)
		if m >= r {
			return engine.Invalid("min.insync.replicas (%d) must be lower than default.replication.factor (%d)", m, r)
		}
	}
	return nil
}

// effectiveConfig merges platform defaults with validated customer config.
// Values are typed the way Strimzi expects (ints and bools as JSON scalars).
func effectiveConfig(cfg map[string]string) map[string]any {
	out := map[string]any{}
	for k, v := range platformConfig {
		out[k] = v
	}
	for k, v := range cfg {
		rule := allowedConfig[k]
		switch rule.kind {
		case "int":
			n, _ := strconv.ParseInt(v, 10, 64)
			out[k] = n
		case "bool":
			out[k] = v == "true"
		default:
			out[k] = v
		}
	}
	return out
}
