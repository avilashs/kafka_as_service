package agent

import "testing"

const sample = `# HELP kafka_server_brokertopicmetrics_bytesin_total Total bytes
# TYPE kafka_server_brokertopicmetrics_bytesin_total counter
kafka_server_brokertopicmetrics_bytesin_total 1000
kafka_server_brokertopicmetrics_bytesin_total{topic="orders"} 700.0
kafka_server_brokertopicmetrics_bytesin_total{topic="__platform_canary"} 300
kafka_server_replicamanager_underminisrpartitioncount{} 2
other_metric{a="x,y",b="say \"hi\"} "} 5 1700000000
kafka_server_brokertopicmetrics_bytesout_total{topic="orders"} 50
kafka_server_brokertopicmetrics_bytesout_total{topic="b"} 25
`

func TestParseAndAggregate(t *testing.T) {
	ss := parseProm(sample)
	if len(ss) != 7 {
		t.Fatalf("parsed %d series: %+v", len(ss), ss)
	}
	var odd series
	for _, s := range ss {
		if s.Name == "other_metric" {
			odd = s
		}
	}
	if odd.Labels["a"] != "x,y" || odd.Labels["b"] != `say "hi"} ` || odd.Value != 5 {
		t.Errorf("label parsing: %+v", odd)
	}
	// aggregate minus the excluded topic: the platform's own traffic is not billed
	if v, ok := metricValue(ss, "kafka_server_brokertopicmetrics_bytesin_total", []string{"__platform_canary"}); !ok || v != 700 {
		t.Errorf("bytes in = %v %v, want 700", v, ok)
	}
	// without an aggregate series the per-topic series are summed
	if v, ok := metricValue(ss, "kafka_server_brokertopicmetrics_bytesout_total", nil); !ok || v != 75 {
		t.Errorf("bytes out = %v %v, want 75", v, ok)
	}
	if v, ok := metricValue(ss, "kafka_server_replicamanager_underminisrpartitioncount", nil); !ok || v != 2 {
		t.Errorf("under min isr = %v %v", v, ok)
	}
	if _, ok := metricValue(ss, "absent", nil); ok {
		t.Error("an absent metric reported a value")
	}
}
