package luckyagent

import (
	"testing"

	"google.golang.org/protobuf/types/descriptorpb"
)

func TestProtocolKeepsWireNameAndUsesIndependentModule(t *testing.T) {
	if got := string(File_luckyagent_proto.Services().Get(0).FullName()); got != "luckyagent.LuckyAgentService" {
		t.Fatalf("wire service changed: %s", got)
	}
	options := File_luckyagent_proto.Options().(*descriptorpb.FileOptions)
	if got := options.GetGoPackage(); got != "github.com/yurika0211/luckyagent-api/grpc;luckyagent" {
		t.Fatalf("generated descriptor package = %s", got)
	}
}
