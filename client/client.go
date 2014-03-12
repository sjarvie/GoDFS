// Package datanode contains the functionality to run the client in GoDFS
package client

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strings"
	"sync"
)

const SERVERADDR = "localhost:8080"
const SIZEOFBLOCK = 1000

var id string                        // client id
var state = HB                       // internal statemachine
var sendChannel chan Packet          // for outbound Packets
var receiveChannel chan Packet       // for in bound Packets
var sendMap map[string]*json.Encoder // maps DatanodeIDs to their connections
var sendMapLock sync.Mutex

var encoder *json.Encoder
var decoder *json.Decoder

// commands for node communication
const (
	HB            = iota // heartbeat
	LIST          = iota // list directorys
	ACK           = iota // acknowledgement
	BLOCK         = iota // handle the incoming Block
	BLOCKACK      = iota // notifcation that Block was written to disc
	RETRIEVEBLOCK = iota // request to retrieve a Block
	DISTRIBUTE    = iota // request to distribute a Block to a datanode
	GETHEADERS    = iota
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

// SendPackets encodes packets and transmits them to their proper recipients
func SendPackets(encoder *json.Encoder, ch chan Packet) {
	for p := range ch {
		err := encoder.Encode(p)
		if err != nil {
			log.Println("error sending", p.DST)
		}
	}
}

// SendHeartbeat is used to notify the namenode of a valid connection
// on a periodic basis
func SendHeartbeat() {
	p := new(Packet)
	p.SRC = id
	p.DST = "NN"
	p.CMD = HB

	encoder.Encode(*p)
}

// BlocksHeadersFromFile generates Blockheaders without datanodeID assignments
// The client uses these headers to write blocks to datanodes
func BlockHeadersFromFile(localname, remotename string) []BlockHeader {

	var headers []BlockHeader

	info, err := os.Lstat(localname)
	if err != nil {
		panic(err)
	}

	// get read buffer
	fi, err := os.Open(localname)
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := fi.Close(); err != nil {
			panic(err)
		}
	}()

	// Create Blocks
	numblocks := int((info.Size() / SIZEOFBLOCK) + 1)
	headers = make([]BlockHeader, numblocks, numblocks)
	blocknum := 0

	for blocknum < numblocks {
		if strings.Index(remotename, "/") != 0 {
			remotename = "/" + remotename
		}

		n := uint64(SIZEOFBLOCK)
		if blocknum == (numblocks - 1) {
			n = uint64(info.Size()) % SIZEOFBLOCK

		}

		h := BlockHeader{"", remotename, n, blocknum, numblocks}

		// load balance via roundrobin
		blocknum++
		headers[blocknum] = h
	}

	return headers
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

// BlocksFromFile split a File into Blocks for storage on the filesystem
// in the future this will read a fixed number of blocks at a time from disc for reasonable memory utilization
func BlocksFromFile(localname, remotename string) []Block {

	var bls []Block

	info, err := os.Lstat(localname)
	if err != nil {
		panic(err)
	}

	// get read buffer
	fi, err := os.Open(localname)
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := fi.Close(); err != nil {
			panic(err)
		}
	}()
	r := bufio.NewReader(fi)

	// Create Blocks
	total := int((info.Size() / SIZEOFBLOCK) + 1)
	bls = make([]Block, 0, total)

	num := 0

	for {

		// read a chunk from file uint64o Block data buffer
		buf := make([]byte, SIZEOFBLOCK)
		w := bytes.NewBuffer(nil)

		n, err := r.Read(buf)
		if err != nil && err != io.EOF {
			panic(err)
		}
		if n == 0 {
			break
		}

		if _, err := w.Write(buf[:n]); err != nil {
			panic(err)
		}

		// write full Block to disc
		if strings.Index(remotename, "/") != 0 {
			remotename = "/" + remotename
		}

		h := BlockHeader{"", remotename, uint64(n), num, total}

		data := make([]byte, 0, n)
		data = w.Bytes()[0:n]
		b := Block{h, data}
		bls = append(bls, b)

		// generate new Block
		num += 1

	}
	return bls
}

type errorString struct {
	s string
}

func (e *errorString) Error() string {
	return e.s
}

// New returns an error that formats as the given text.
func New(text string) error {
	return &errorString{text}
}

func DistributeBlocks(blocks []Block) error {

	for _, b := range blocks {
		p := new(Packet)
		p.SRC = id
		p.DST = "NN"
		p.CMD = DISTRIBUTE
		p.Data = b
		encoder.Encode(*p)

		var r Packet
		decoder.Decode(&r)
		if r.CMD != ACK {
			return errors.New("Could not distribute block to namenode")
		}
	}
	return nil
}

// RetrieveFile queries the filesystem for the File located at remotename,
// and saves its contents to the file localname
func RetrieveFile(localname, remotename string) {
	// TODO make this handle errors gracefully
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("Recovered from panic ", r)
			fmt.Println("Unable to retrieve file")
			return
		}
	}()
	// send header request
	p := new(Packet)
	p.DST = "NN"
	p.SRC = id
	p.CMD = GETHEADERS
	p.Headers = make([]BlockHeader, 1, 1)
	p.Headers[0] = BlockHeader{"", remotename, uint64(0), 0, 0}
	encoder.Encode(*p)

	// get header list
	var r Packet
	decoder.Decode(&r)
	if r.CMD != GETHEADERS || r.Headers == nil {
		fmt.Println("Bad response packet for GETHEADERS ", r)
		return
	}

	// setup writer
	outFile, err := os.Create(localname)
	if err != nil {
		fmt.Println("error constructing file: ", err)
		return
	}
	defer outFile.Close()
	w := bufio.NewWriter(outFile)

	// for each header, retrieve its block and write to disc
	headers := r.Headers

	fmt.Println("Received File Headers for ", p.Headers[0].Filename, ". Retrieving Blocks ")
	for _, h := range headers {

		// send request
		p := new(Packet)
		p.DST = "NN"
		p.SRC = id
		p.CMD = RETRIEVEBLOCK
		p.Headers = make([]BlockHeader, 1, 1)
		p.Headers[0] = h
		encoder.Encode(*p)

		// receive block
		var r Packet
		decoder.Decode(&r)

		if r.CMD != BLOCK {
			fmt.Println("Received bad packet ", p)
			return
		}

		b := r.Data
		n := b.Header.Size
		_, err := w.Write(b.Data[:n])
		if err != nil {
			panic(err)
		}
	}
	w.Flush()
	fmt.Println("Wrote file to disc at ", localname)
}

// ReceiveInput provides user interaction and file placement/retrieval from remote filesystem
func ReceiveInput() {
	for {
		fmt.Println("Enter command to send")

		var cmd string
		var file1 string
		var file2 string
		fmt.Scan(&cmd)
		fmt.Scan(&file1)
		fmt.Scan(&file2)

		if !(cmd == "put" || cmd == "get") {
			fmt.Printf("Incorrect command\n Valid Commands: \n \t put [localinput] [remoteoutput] \n \t get [remoteinput] [localoutput] \n")
			continue
		}

		if cmd == "put" {

			localname := file1
			remotename := file2
			_, err := os.Lstat(localname)
			if err != nil {
				fmt.Println("File ", localname, " could not be accessed")
				continue
			}

			// generate blocks from new File for distribution
			blocks := BlocksFromFile(localname, remotename)

			fmt.Println("Distributing ", len(blocks), "file blocks")

			// send blocks to namenode for distribution

			err = DistributeBlocks(blocks)
			if err != nil {
				fmt.Println(err)
				continue
			}

		} else {
			remotename := file1
			localname := file2
			fmt.Println("Retrieving file")
			RetrieveFile(localname, remotename)

		}
	}

}

// Initializes the client and begins communication
func Init() {

	id = "C"
	conn, err := net.Dial("tcp", SERVERADDR)
	CheckError(err)

	encoder = json.NewEncoder(conn)
	decoder = json.NewDecoder(conn)

	// Start communication
	//	SendHeartbeat()
	ReceiveInput()

	os.Exit(0)
}

func CheckError(err error) {
	if err != nil {
		fmt.Println("Fatal error ", err.Error())
		os.Exit(1)
	}
}
