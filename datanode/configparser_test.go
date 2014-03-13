package datanode

import (
	"testing"
)

func TestValidXML(t *testing.T) {

	err := ParseConfigXML("exampledatanode.xml")
	if err != nil {
		t.Errorf(err.Error())
	}

	if id != "DN1" {
		t.Errorf("Config did not set id correctly")
	}

	if root != "/DN1" {
		t.Errorf("Config did not set id correctly")
	}

	if serverhost != "localhost" {
		t.Errorf("Config did not set host correctly")
	}

	if serverport != "8080" {
		t.Errorf("Config did not set port correctly")
	}

	if SIZEOFBLOCK != 4096 {
		t.Errorf("Config did not set SIZEOFBLOCK correctly")
	}

}
