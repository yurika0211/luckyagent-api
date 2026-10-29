package websocket

import (
	"encoding/json"
	"testing"
)

func TestUIDeltaRoundTrip(t *testing.T) {
	msg, err := NewMessage(TypeUIDelta, "s1", UIDeltaData{
		TreeID: "t1",
		State:  "complete",
		Nodes: []UINodeData{{
			ID: "a", Type: "Table", Policy: "inspect",
			Props: map[string]any{"columns": []string{"n"}},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if msg.Type != TypeUIDelta {
		t.Fatalf("type %s", msg.Type)
	}
	var data UIDeltaData
	if err := json.Unmarshal(msg.Data, &data); err != nil {
		t.Fatal(err)
	}
	if data.TreeID != "t1" || len(data.Nodes) != 1 || data.Nodes[0].Type != "Table" {
		t.Fatalf("%#v", data)
	}
}

func TestUIActionRoundTrip(t *testing.T) {
	msg, err := NewMessage(TypeUIAction, "s1", UIActionData{TreeID: "t1", WidgetID: "go"})
	if err != nil {
		t.Fatal(err)
	}
	var data UIActionData
	if err := json.Unmarshal(msg.Data, &data); err != nil {
		t.Fatal(err)
	}
	if data.WidgetID != "go" {
		t.Fatalf("%#v", data)
	}
}
