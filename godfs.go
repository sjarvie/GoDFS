package main

import (
	"fmt"
	"github.com/sjarvie/godfs/datanode"
	"github.com/sjarvie/godfs/namenode"
	"github.com/sjarvie/godfs/client"
	"os"
)

func main() {

	if ! ( (len(os.Args) == 2 && (os.Args[1] == "namenode" || os.Args[1] == "client")) || (len(os.Args) == 4 && os.Args[1] == "datanode")) {
		fmt.Println("Invalid command, usage : ")
		fmt.Println(" \t godfs namenode ")
		fmt.Println(" \t godfs datanode [datanodeID] [absolute_block_path]  ")
		fmt.Println(" \t godfs client ")

		os.Exit(1)
	}

	cmd := os.Args[1]

	if cmd == "namenode" {
		namenode.Init()
	} else if cmd == "datanode" {
		id := os.Args[2]
		fspath := os.Args[3]
		datanode.Init(id, fspath)
	} else if cmd == "client" {
		client.Init()

	}
}
