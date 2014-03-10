package main


import (
	"fmt"
	"os"
	"github.com/sjarvie/godfs/namenode"
	"github.com/sjarvie/godfs/datanode"
	)


func main() {

	if !((len(os.Args) == 2 && os.Args[1] == "namenode") || (len(os.Args) == 4 && os.Args[1] == "datanode")) {
		fmt.Printf("Invalid command, usage :\n \t godfs namenode \n \t godfs datanode [datanodeID] [absolute_block_path]  \n")
		os.Exit(1)
	}
	


	cmd := os.Args[1]
	
	if cmd == "namenode" {
		namenode.Init()
	} else if cmd == "datanode" {
		id := os.Args[2]
		fspath := os.Args[3]
		datanode.Init(id,fspath)
	} 
}