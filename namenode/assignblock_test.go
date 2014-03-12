package namenode

import (
	"testing"
)

func TestInvalidBlockInput(t *testing.T) {

	Init("examplenamenode.xml")

	dn1 := datanode{"DN1", true, 0}
	datanodemap["DN1"] = &dn1

	// Test a bad block
	var b1 Block

	_, err := AssignBlock(b1)

	if err == nil {
		t.Errorf("Distributed invalid Block")
	}
}

func TestValidBlockInput(t *testing.T) {

	Init("examplenamenode.xml")

	dn1 := datanode{"DN1", true, 0}
	datanodemap["DN1"] = &dn1

	// Test a bad block
	var b1 Block

	inh := BlockHeader{"DN1", "/out.txt", 1, 0, 1}
	b1.Header = inh
	b1.Data = make([]byte, 1, 1)

	_, err := AssignBlock(b1)

	if err != nil {
		t.Errorf("Could not insert valid block")
	}
}
