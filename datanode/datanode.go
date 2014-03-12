// Package datanode contains the functionality to run a datanode in GoDFS
package datanode

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

const SERVERADDR = "localhost:8080"
const SIZEOFBLOCK = 1000

var id string   // datanode id
var root string // root of the block filesystem
var state = HB  // internal statemachine

// commands for node communication
const (
	HB            = iota // heartbeat
	LIST          = iota // list directorys
	ACK           = iota // acknowledgement
	BLOCK         = iota // handle the incoming Block
	BLOCKACK      = iota // notifcation that Block was written to disc
	RETRIEVEBLOCK = iota // request to retrieve a Block
	DISTRIBUTE    = iota // request to distribute a Block to a datanode
	GETHEADERS    = iota // request to retrieve the headers of a given filename
	ERROR         = iota // notification of a failed request
)

// A file is composed of one or more Blocks
type Block struct {
	Header BlockHeader // metadata
	Data   []byte      // data contents
}

// Blockheaders hold Block metadata
type BlockHeader struct {
	DatanodeID string // ID of datanode which holds the block
	Filename   string //the remote name of the block including the path "/test/0"
	Size       uint64 // size of Block in bytes
	BlockNum   int    // the 0 indexed position of Block within file
	NumBlocks  int    // total number of Blocks in file
}

// Packets are sent over the network
type Packet struct {
	SRC     string        // source ID
	DST     string        // destination ID
	CMD     int           // command for the handler
	Data    Block         // optional Block
	Headers []BlockHeader // optional Blockheader list
}

// ReceivePacket decodes a packet and adds it to the handler channel
// for processing by the datanode
func ReceivePackets(decoder *json.Decoder, p chan Packet) {
	for {
		r := new(Packet)
		decoder.Decode(r)
		p <- *r
	}
}

// SendHeartbeat is used to notify the namenode of a valid connection
// on a periodic basis
func SendHeartbeat(encoder *json.Encoder) {
	p := new(Packet)
	p.SRC = id
	p.DST = "NN"
	p.CMD = HB
	encoder.Encode(p)
}

// HandleResponse delegates actions to perform based on the
// contents of a recieved Packet, and encodes a response
func HandleResponse(p Packet, encoder *json.Encoder) {
	r := new(Packet)
	r.SRC = id
	r.DST = p.SRC

	switch p.CMD {
	case ACK:
		return
	case LIST:
		list := GetBlockHeaders()
		r.Headers = make([]BlockHeader, len(list))

		for i, b := range list {
			r.Headers[i] = b
		}
		r.CMD = LIST
	case BLOCK:
		r.CMD = BLOCKACK
		WriteBlock(p.Data)
		r.Headers = make([]BlockHeader, 0, 2)
		r.Headers = append(r.Headers, p.Data.Header)

	case RETRIEVEBLOCK:
		fmt.Println("retrieving block from ", p.Headers[0])
		b := BlockFromHeader(p.Headers[0])
		r.CMD = BLOCK
		r.Data = b
	}
	encoder.Encode(*r)
}

// WriteBlock performs all functionality necessary to write a Block b
// to the local filesystem
func WriteBlock(b Block) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("Recovered from panic ", r)
			fmt.Println("Unable to write Block")
			return
		}
	}()

	list, err := ioutil.ReadDir(root)
	h := b.Header
	fname := h.Filename + "/" + strconv.Itoa(h.BlockNum)

	for _, dir := range list {
		fmt.Println("Looking for directory ", h.Filename)
		fmt.Println("Comparing to ", dir.Name())
		if "/"+dir.Name() == h.Filename {
			WriteJSON(root+fname, b)
			return
		}
	}
	// create directory
	err = os.Mkdir(root+h.Filename, 0700)
	if err != nil {
		fmt.Println("path error : ", err)
		return
	}

	fname = h.Filename + "/" + strconv.Itoa(h.BlockNum)
	WriteJSON(root+fname, b)
	log.Println("Wrote Block ", fname, "to disc")
	return

}

// GetBlockHeaders retrieves the list of all Blockheaders found within
// the filesystem specified by the user.
func GetBlockHeaders() []BlockHeader {

	list, err := ioutil.ReadDir(root)

	CheckError(err)
	headers := make([]BlockHeader, 0, len(list))

	// each directory is Filename, which holds Block files within
	for _, dir := range list {

		fmt.Println(dir.Name())

		files, err := ioutil.ReadDir(dir.Name())
		if err != nil {
			log.Println("Error reading directory ", err)
		} else {
			for _, fi := range files {
				var b Block
				fpath := strings.Join([]string{root, dir.Name(), fi.Name()}, "/")
				ReadJSON(fpath, &b)
				headers = append(headers, b.Header)
			}
		}
	}
	return headers
}

// BlockFromHeader retrieves a Block using metadata from the Blockheader h
func BlockFromHeader(h BlockHeader) Block {

	list, err := ioutil.ReadDir(root)
	var errBlock Block
	if err != nil {
		log.Println("Error reading directory ", err)
		return errBlock
	}

	fname := h.Filename + "/" + strconv.Itoa(h.BlockNum)
	for _, dir := range list {
		var b Block
		if "/"+dir.Name() == h.Filename {
			ReadJSON(root+fname, &b)
			fmt.Println("Found block ", b)
			return b
		}
	}
	fmt.Println("Block not found!")
	return errBlock
}

// ReadJSON reads a JSON encoded interface to disc
func ReadJSON(fname string, key interface{}) {
	fi, err := os.Open(fname)
	defer fi.Close()

	if err != nil {
		fmt.Println("Couldnt Read JSON ")
		return
	}

	decoder := json.NewDecoder(fi)
	err = decoder.Decode(key)
	if err != nil {
		log.Println("Error encoding JSON ", err)
		return
	}
}

// WriteJSON writes a JSON encoded interface to disc
func WriteJSON(fileName string, key interface{}) {
	outFile, err := os.Create(fileName)
	defer outFile.Close()
	if err != nil {
		log.Println("Error opening JSON ", err)
		return
	}
	encoder := json.NewEncoder(outFile)
	err = encoder.Encode(key)
	if err != nil {
		log.Println("Error encoding JSON ", err)
		return
	}
}

// Init initializes all internal structures to run a datanode
// The id of the node is dn_id and fspath is the location to
// use as the block storage system on disc
func Init(dn_id, fspath string) {

	id = dn_id
	root = fspath

	err := os.Chdir(root)
	CheckError(err)
	conn, err := net.Dial("tcp", SERVERADDR)
	CheckError(err)

	encoder := json.NewEncoder(conn)
	decoder := json.NewDecoder(conn)

	PacketChannel := make(chan Packet)
	go ReceivePackets(decoder, PacketChannel)

	tick := time.Tick(2 * time.Second)

	// start communication
	for {
		select {
		case <-tick:
			SendHeartbeat(encoder)
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
