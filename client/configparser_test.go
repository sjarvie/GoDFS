package client

import (
	"testing"
)

func TestValidXML(t *testing.T) {

	err := ParseConfigXML("exampleclient.xml")
	if err != nil {
		t.Errorf(err.Error())
	}
	// Single client for now
	//	if id != "C" {
	//		t.Errorf("Config did not set id correctly")
	//	}

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
