package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"time"
)

const SERVERADDR = "localhost:8080"
const SIZEOFBLOCK = 1000

var id, blockpath string // command line arguments
var state = HB
const (
	HB = iota
	LIST = iota
	ACK = iota
	BLOCK = iota
)

// A file is composed of one or more blocks
type block struct {
	header blockheader
	data   []byte
}

// block headers hold block data information
type blockheader struct {
	datanodeID                   string
	filename                     string //the filename including the path "/data/test.txt"
	size, sequenceNum, numBlocks int    //size in bytes, the order of the chunk within file
	//TODO checksum
}

// packets are sent over the network
type packet struct {
	SRC		 string
	DST		 string
	command  int
	data     block
	headers  []blockheader
}

// receive a packet and send to syncronized channel
func Receivepacket(conn net.Conn, p chan packet) {
	decoder := json.NewDecoder(conn)
	for {
		var r packet
		decoder.Decode(&r)
		fmt.Println(r)
		p <- r
	}
}


// change state to handle request from server
func HandleResponse(r packet) {
	if r.command == ACK {
		state = HB
	} else if r.command == LIST {
		state = LIST
	}
	fmt.Println("state now ", state)

}

// handle a state's action
func PerformAction(encoder *json.Encoder) {
	p := new(packet)
	p.SRC = id
	p.DST = "NN" //TODO better naming conventions

	if state == HB {
		p.command = HB
	} else if state == LIST {
		fmt.Println("Sending block headers")
		list := GetBlockHeaders()
		p.headers = make([]blockheader, len(list))

		for i, b := range list {
			p.headers[i] = b
		}
		p.command = LIST
	}
	//fmt.Println("Sending Packet : ", *p)
	encoder.Encode(p)
}

//func blocksFromFile

// List block contents
// Later add recursion and switch to blocks
func GetBlockHeaders() []blockheader {

	list, err := ioutil.ReadDir(blockpath)
	CheckError(err)
	headers := make([]blockheader, 0, len(list))

	for i, f := range list {
		var b block
		Readjson(f.Name(), &b)
		headers[i] = b.header
	}
	return headers
}

func Readjson(fname string, key interface{}) {
	fi, err := os.Open(fname)
	CheckError(err)
	decoder := json.NewDecoder(fi)
	err = decoder.Decode(key)
	CheckError(err)
	fi.Close()
}

func Writejson(fileName string, key interface{}) {
	outFile, err := os.Create(fileName)
	CheckError(err)
	encoder := json.NewEncoder(outFile)
	err = encoder.Encode(key)
	CheckError(err)
	outFile.Close()
}

func main() {

	if len(os.Args) != 3 {
	    fmt.Println("Usage: ", os.Args[0], "id path")
	    os.Exit(1)
	}
	id = os.Args[1]
	blockpath = os.Args[2]

	conn, err := net.Dial("tcp", SERVERADDR)
	CheckError(err)

	encoder := json.NewEncoder(conn)

	packetChannel := make(chan packet)
	tick := time.Tick(2* time.Second)

	go Receivepacket(conn, packetChannel)

	for {
		select {
			case <-tick:
				PerformAction(encoder)
			case r := <-packetChannel:
				HandleResponse(r)
				PerformAction(encoder)
				time.Sleep(1*time.Second)
		}

	}
	os.Exit(0)
}

func CheckError(err error) {
	if err != nil {
		fmt.Println("Fatal error ", err.Error())
		//os.Exit(1)
	}
}
