package namenode

import (
	"testing"
)

func TestValidXML(t *testing.T) {

	err := ParseConfigXML("examplenamenode.xml")
	if err != nil {
		t.Errorf(err.Error())
	}

	if id != "NN" {
		t.Errorf("Config did not set id correctly")
	}

	if host != "localhost" {
		t.Errorf("Config did not set host correctly")
	}

	if port != "8080" {
		t.Errorf("Config did not set port correctly")
	}

	if SIZEOFBLOCK != 1000 {
		t.Errorf("Config did not set SIZEOFBLOCK correctly")
	}

}
