package main

import (
	"fmt"
	"github.com/sjarvie/godfs/client"
	"github.com/sjarvie/godfs/datanode"
	"github.com/sjarvie/godfs/namenode"
	"os"
)

func main() {

	if !(len(os.Args) == 3 && (os.Args[1] == "namenode" || os.Args[1] == "client" || os.Args[1] == "datanode")) {
		fmt.Println("Invalid command, usage : ")
		fmt.Println(" \t godfs namenode [location of namenode configuration file] ")
		fmt.Println(" \t godfs datanode [location of datanode configuration file] ")
		fmt.Println(" \t godfs client [location of client configuration file] ")

		os.Exit(1)
	}

	cmd := os.Args[1]
	configpath := os.Args[2]

	switch cmd {
	case "namenode":
		namenode.Run(configpath)
	case "datanode":
		datanode.Run(configpath)
	case "client":
		fmt.Println(configpath)
		client.Run(configpath)
	}

}
