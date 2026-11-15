package aws_test

import (
	"bytes"
	"encoding/json"
	"os"
	"regexp"
	"testing"

	"github.com/avilashs/kafka_as_service/internal/platform/store"
)

// The registration template of an AWS cell must load into the structure the control plane registers cells with,
// with no field it does not know (a mistyped field name would otherwise be dropped silently).
func TestCellTemplateLoadsIntoCellSpec(t *testing.T) {
	raw, err := os.ReadFile("cell.json.tmpl")
	if err != nil {
		t.Fatal(err)
	}
	vars := map[string]string{
		"CELL_ID": "aws-1", "REGION": "us-east-1", "DOMAIN": "streams.example.net",
		"ZONES_JSON": `["us-east-1a","us-east-1b","us-east-1c"]`, "APISERVER_CIDRS_JSON": `["10.0.0.5/32"]`,
		"ENGINE_IMAGES_JSON": `{"4.3.1":"registry.example/kafka:4.3.1"}`, "CONTROL_INSTANCE_TYPE": "m7i.large",
		"SHARED_INSTANCE_TYPE": "m7i.2xlarge", "SHARED_MAX_NODES": "9", "DEDICATED_INSTANCE_TYPE": "m7i.xlarge",
		"DEDICATED_MAX_NODES": "9", "DEDICATED_CPU_MILLI": "3800", "DEDICATED_MEM_MIB": "14500",
		"CONTROL_ZONES_JSON":   `[{"zone":"us-east-1a","cpuMilli":1900,"memoryMiB":7000,"ebsMBps":150,"attach":24,"nodes":1}]`,
		"SHARED_ZONES_JSON":    `[{"zone":"us-east-1a","cpuMilli":7600,"memoryMiB":29000,"ebsMBps":150,"attach":24,"nodes":0}]`,
		"DEDICATED_ZONES_JSON": `[{"zone":"us-east-1a","cpuMilli":0,"memoryMiB":0,"ebsMBps":0,"attach":0,"nodes":0}]`,
	}
	rendered := regexp.MustCompile(`\$\{(\w+)\}`).ReplaceAllFunc(raw, func(m []byte) []byte {
		v, ok := vars[string(m[2:len(m)-1])]
		if !ok {
			t.Errorf("variable %s is not given by up.sh", m)
		}
		return []byte(v)
	})
	var c store.CellSpec
	dec := json.NewDecoder(bytes.NewReader(rendered))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&c); err != nil {
		t.Fatalf("%v\n%s", err, rendered)
	}
	if c.ID != "aws-1" || len(c.Pools) != 3 || c.Env.StorageClass != "gp3" || c.Env.OperatorNS != "streamcell-operators" {
		t.Errorf("unexpected cell: %+v", c)
	}
}
