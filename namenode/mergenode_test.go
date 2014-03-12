package namenode

import (
	"strconv"
	"testing"
)

func TestSingleInsert(t *testing.T) {

	filemap = make(map[string]map[int][]BlockHeader)
	root = &filenode{"/", nil, make([]*filenode, 0, 5)}
	datanodemap = make(map[string]*datanode)

	dn1 := datanode{"DN1", true, 0}
	datanodemap["DN1"] = &dn1

	// Test a file that exists
	inh := BlockHeader{"DN1", "/out.txt", 1, 0, 1}
	MergeNode(inh)
	_, ok := filemap["/out.txt"]
	if !ok {
		t.Errorf("merge failed ")
	}
}

func TestMultipleInsertsSameFile(t *testing.T) {

	filemap = make(map[string]map[int][]BlockHeader)
	root = &filenode{"/", nil, make([]*filenode, 0, 5)}
	datanodemap = make(map[string]*datanode)

	dn1 := datanode{"DN1", true, 0}
	datanodemap["DN1"] = &dn1

	// Test handling multiple blocks
	inh1 := BlockHeader{"DN1", "/out.txt", 1, 0, 2}
	inh2 := BlockHeader{"DN1", "/out.txt", 1, 1, 2}

	err := MergeNode(inh1)
	if err != nil {
		t.Errorf("%s", err)
	}

	err = MergeNode(inh2)
	if err != nil {
		t.Errorf("%s", err)
	}

	blks, ok := filemap["/out.txt"]
	if !ok {
		t.Errorf("merge failed ")
	}
	harr1, ok := blks[0]
	if !ok {
		t.Errorf("merge failed ")
	}

	if len(harr1) != 1 {
		t.Errorf("merge failed: Expected 1 , Got " + strconv.Itoa(len(harr1)))
	}

	if harr1[0] != inh1 {
		t.Errorf("merge failed ")
	}

	harr2, ok := blks[1]
	if !ok {
		t.Errorf("merge failed ")
	}

	if len(harr2) != 1 {
		t.Errorf("merge failed: Expected 1 , Got " + strconv.Itoa(len(harr2)))
	}

	if harr2[0] != inh2 {
		t.Errorf("merge failed ")
	}
}

func TestDuplicateInsert(t *testing.T) {
	filemap = make(map[string]map[int][]BlockHeader)
	root = &filenode{"/", nil, make([]*filenode, 0, 5)}
	datanodemap = make(map[string]*datanode)

	dn1 := datanode{"DN1", true, 0}
	datanodemap["DN1"] = &dn1

	inh := BlockHeader{"DN1", "/out.txt", 1, 0, 1}
	err := MergeNode(inh)

	if err != nil {
		t.Errorf("%s", err)
	}
	_, ok := filemap["/out.txt"]
	if !ok {
		t.Errorf("merge failed ")
	}
	err = MergeNode(inh)
	if err != nil {
		t.Errorf("%s", err)
	}

	blks, ok := filemap["/out.txt"]
	if !ok {
		t.Errorf("merge failed ")
	}
	harr1, ok := blks[0]

	if len(harr1) != 1 {
		t.Errorf("merge failed: Expected 1 , Got " + strconv.Itoa(len(harr1)))
	}

}
