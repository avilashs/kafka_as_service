package azure_test

import (
	"bytes"
	"encoding/json"
	"os"
	"regexp"
	"testing"

	"github.com/avilashs/kafka_as_service/internal/platform/store"
)

// The registration template of an Azure cell must load into the structure the control plane registers cells with,
// with no field it does not know (a mistyped field name would otherwise be dropped silently).
func TestCellTemplateLoadsIntoCellSpec(t *testing.T) {
	raw, err := os.ReadFile("cell.json.tmpl")
	if err != nil {
		t.Fatal(err)
	}
	vars := map[string]string{
		"CELL_ID": "az-1", "LOCATION": "eastus2", "DOMAIN": "streams.example.net",
		"ZONES_JSON": `["eastus2-1","eastus2-2","eastus2-3"]`, "APISERVER_CIDRS_JSON": `["10.0.0.5/32"]`,
		"ENGINE_IMAGES_JSON": `{"4.3.1":"registry.example/kafka:4.3.1"}`, "CONTROL_VM_SIZE": "Standard_D2ds_v5",
		"SHARED_VM_SIZE": "Standard_D8ds_v5", "SHARED_MAX_NODES": "9", "DEDICATED_VM_SIZE": "Standard_D4ds_v5",
		"DEDICATED_MAX_NODES": "9", "DEDICATED_CPU_MILLI": "3600", "DEDICATED_MEM_MIB": "13500",
		"CONTROL_ZONES_JSON":   `[{"zone":"eastus2-1","cpuMilli":1600,"memoryMiB":5500,"ebsMBps":100,"attach":4,"nodes":1}]`,
		"SHARED_ZONES_JSON":    `[{"zone":"eastus2-1","cpuMilli":7100,"memoryMiB":27000,"ebsMBps":190,"attach":16,"nodes":1}]`,
		"DEDICATED_ZONES_JSON": `[{"zone":"eastus2-1","cpuMilli":0,"memoryMiB":0,"ebsMBps":0,"attach":0,"nodes":0}]`,
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
	if c.ID != "az-1" || len(c.Pools) != 3 || c.Env.StorageClass != "premium-ssd" || c.Env.OperatorNS != "streamcell-operators" {
		t.Errorf("unexpected cell: %+v", c)
	}
}
