package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"time"
	"strconv"
)

const SERVERADDR = "localhost:8080"
const SIZEOFBLOCK = 1000

var id, root string // CMD line arguments
var state = HB

const (
	HB    = iota
	LIST  = iota
	ACK   = iota
	BLOCK = iota
	BLOCKACK = iota
	GETBLOCK = iota
)

// A file is composed of one or more Blocks
type Block struct {
	Header BlockHeader
	Data   []byte
}

// Block Headers hold Block Data information
type BlockHeader struct {
	DatanodeID                   string
	Filename                     string //the Filename including the path "/data/test.txt"
	Size    			int64 //Size of Block in bytes
	BlockNum  int // the 0 indexed position of Block within file
	NumBlocks int  // total number of Blocks in file
}

// Packets are sent over the network
type Packet struct {
	SRC     string
	DST     string
	CMD     int
	Data    Block
	Headers []BlockHeader
}

// receive a Packet and send to syncronized channel
func ReceivePacket(decoder *json.Decoder, p chan Packet) {
	for {
		r := new(Packet)
		decoder.Decode(r)
		p <- *r
	}
}

// change state to handle request from server
func SendHeartBeat(encoder *json.Encoder) {
	p := new(Packet)
	p.SRC = id
	p.DST = "NN"
	p.CMD = HB
	encoder.Encode(p)
}

// handle a state's action
func HandleResponse(p Packet, encoder *json.Encoder) {
	r := new(Packet)
	r.SRC = id
	r.DST = "NN" //TODO better naming conventions
	fmt.Println("Received ", p)

	switch p.CMD {
	case ACK:
		r.CMD = HB
	case LIST:
		list := GetBlockHeaders()
		r.Headers = make([]BlockHeader, len(list))

		for i, b := range list {
			r.Headers[i] = b
		}
		r.CMD = LIST

	case BLOCK:
		fmt.Println("Received Block ",  p.Data)
		r.CMD = BLOCKACK


		WriteBlock(p.Data)

		p.CMD = BLOCKACK
		r.Headers = make([]BlockHeader,0,2)
		r.Headers = append(r.Headers, p.Data.Header)
		LogJSON(*r)

	case GETBLOCK:
		b := BlockFromHeader(p.Headers[0])
		r.CMD = BLOCK
		r.Data = b
		fmt.Println("Sending Block Packet", *r)
	}
	encoder.Encode(*r)
}


func WriteBlock(b Block)  {
	list, err := ioutil.ReadDir(root)
	h := b.Header 
	fname := h.Filename +"/"+ strconv.Itoa(h.BlockNum)

	for _, dir := range list {
		fmt.Println("Looking for directory ", h.Filename)
		fmt.Println("Comparing to ", dir.Name())
		if "/" + dir.Name() == h.Filename{
			SaveJSON(root + fname, b)
			return 
		}
	}
	// create directory
	err = os.Mkdir(root + h.Filename, 0700)
	if (err != nil ){
		fmt.Println("path error : ", err)
		return 
	}

	fname = h.Filename +"/"+ strconv.Itoa(h.BlockNum)
	SaveJSON(root + fname, b)
	fmt.Println("wrote Block to disc")

	return 

}



// List Block contents
// Later add recursion and switch to Blocks
func GetBlockHeaders() []BlockHeader {

	list, err := ioutil.ReadDir(root) 

	CheckError(err)
	headers := make([]BlockHeader, 0, len(list))

	// each directory is Filename, which holds Block files within
	for _, dir := range list {

		fmt.Println(dir.Name())

		files, err := ioutil.ReadDir(dir.Name())
		CheckError(err)

		for _,f := range files {
			var b Block

			ReadJSON(root + "/"+ dir.Name() + "/" + f.Name(), &b)
			headers = append(headers, b.Header)
		}
	}
	return headers
}



func BlockFromHeader(h BlockHeader) Block{
	
	list, err := ioutil.ReadDir(root) 
	CheckError(err)
	fname := h.Filename +"/"+ strconv.Itoa(h.BlockNum)

	for _, dir := range list {
		var b Block

		fmt.Println("Looking for directory ", h.Filename)
		fmt.Println("Comparing to ", "/" + dir.Name())
		if "/" + dir.Name() == h.Filename{
			ReadJSON(root + fname, &b)
			fmt.Println("Found Block!")
			return b 
		}
	}
	fmt.Println("Block not found!")
	var errBlock Block
	return errBlock
}












func ReadJSON(fname string, key interface{}) {
	fi, err := os.Open(fname)
	if err != nil {
		fmt.Println("Couldnt Read JSON ")
		return
	}
	decoder := json.NewDecoder(fi)
	err = decoder.Decode(key)
	CheckError(err)
	fi.Close()
}

func SaveJSON(fileName string, key interface{}) {
	outFile, err := os.Create(fileName)
	CheckError(err)
	encoder := json.NewEncoder(outFile)
	err = encoder.Encode(key)
	CheckError(err)
	outFile.Close()
}

func LogJSON(key interface{}) {
	outFile, err := os.Create("/home/sjarvie/log"+id+".json")
	CheckError(err)
	encoder := json.NewEncoder(outFile)
	err = encoder.Encode(key)
	CheckError(err)
	outFile.Close()
}


func main() {

	if len(os.Args) != 3 {
	    fmt.Println("Usage: datanode id path")
	    os.Exit(1)
	}
	id = os.Args[1]
	root = os.Args[2]
	err := os.Chdir(root)
	CheckError(err)

	conn, err := net.Dial("tcp", SERVERADDR)
	CheckError(err)

	encoder := json.NewEncoder(conn)

	PacketChannel := make(chan Packet)
	tick := time.Tick(2 * time.Second)
	decoder := json.NewDecoder(conn)
	go ReceivePacket(decoder, PacketChannel)

	for {
		select {
		case <-tick:
			SendHeartBeat(encoder)
		case r := <-PacketChannel:
			HandleResponse(r, encoder)
		}

	}
	os.Exit(0)
}

func CheckError(err error) {
	if err != nil {
		fmt.Println("Fatal error ", err.Error())
		os.Exit(1)
	}
}
