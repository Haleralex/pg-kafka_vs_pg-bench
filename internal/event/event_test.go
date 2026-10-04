package event

import (
	"encoding/json"
	"reflect"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestDeterminismAndEncoding(t *testing.T) {
	event := Generate(123456)
	if !reflect.DeepEqual(event, Generate(123456)) {
		t.Fatal("event generation is not deterministic")
	}
	if event.OccurredAt.UnixMilli() != BaseTime.UnixMilli()+123456 {
		t.Fatal("timestamp must preserve id milliseconds")
	}
	data, err := bson.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	var decoded Event
	if err := bson.Unmarshal(data, &decoded); err != nil {
		t.Fatal(err)
	}
	wantJSON, _ := json.Marshal(event)
	gotJSON, _ := json.Marshal(decoded)
	if string(wantJSON) != string(gotJSON) {
		t.Fatalf("JSON changed after BSON round trip:\n%s\n%s", wantJSON, gotJSON)
	}
	if len(event.Payload.Message) < 699 || len(event.Payload.Message) > 720 || event.Payload.Tags[0] == event.Payload.Tags[1] {
		t.Fatal("unexpected payload size or duplicate tags")
	}
	if batch := Batch(123455, 3); !reflect.DeepEqual(batch[1], event) {
		t.Fatal("batch boundaries changed the generated document")
	}
}

func TestDistribution(t *testing.T) {
	servicesByTenant := map[int]map[string]bool{}
	errors := 0
	for id := int64(1); id <= 10000; id++ {
		e := Generate(id)
		if servicesByTenant[e.TenantID] == nil {
			servicesByTenant[e.TenantID] = map[string]bool{}
		}
		servicesByTenant[e.TenantID][e.Payload.Service] = true
		if e.Payload.Level == "error" {
			errors++
		}
	}
	if errors < 850 || errors > 1150 || len(servicesByTenant) != 100 {
		t.Fatalf("unexpected weighted distribution: errors=%d tenants=%d", errors, len(servicesByTenant))
	}
	for tenant, services := range servicesByTenant {
		if len(services) < 16 {
			t.Fatalf("tenant %d sees only %d services; generator may correlate fields", tenant, len(services))
		}
	}
}

func TestValidRangeRejectsOverflow(t *testing.T) {
	if !ValidRange(MaxID, 1) || ValidRange(MaxID, 2) || ValidRange(1, 0) || ValidRange(-1, 1) {
		t.Fatal("invalid id range validation")
	}
}
