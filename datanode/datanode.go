// Package datanode contains the functionality to run a datanode in GoDFS
package datanode

import (
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config Options
var serverhost string // server host
var serverport string // server port
var SIZEOFBLOCK int64 // size of block in bytes
var id string         // the datanode id
var root string       // the root location on disk to store Blocks

var state = HB // internal statemachine

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

// The XML parsing structures for configuration options
type ConfigOptionList struct {
	XMLName       xml.Name       `xml:"ConfigOptionList"`
	ConfigOptions []ConfigOption `xml:"ConfigOption"`
}

type ConfigOption struct {
	Key   string `xml:"key,attr"`
	Value string `xml:",chardata"`
}

// A file is composed of one or more Blocks
type Block struct {
	Header BlockHeader // metadata
	Data   []byte      // data contents
}

// Blockheaders hold Block metadata
type BlockHeader struct {
	DatanodeID string // ID of datanode which holds the block
	Filename   string //the remote name of the block including the path "/test/0"
	Size       int64  // size of Block in bytes
	BlockNum   int    // the 0 indexed position of Block within file
	NumBlocks  int    // total number of Blocks in file
}

// Packets are sent over the network
type Packet struct {
	SRC     string        // source ID
	DST     string        // destination ID
	CMD     int           // command for the handler
	Message string        // optional packet contents explanation
	Data    Block         // optional Block
	Headers []BlockHeader // optional BlockHeader list
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

		if "/"+dir.Name() == h.Filename {
			WriteJSON(root+fname, b)
			log.Println("Wrote Block ", root+fname, "to disc")
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
	log.Println("Wrote Block ", root+fname, "to disc")
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
			return b
		}
	}
	fmt.Println("Block not found ", root+fname)
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

// Parse Config sets up the node with the provided XML file
func ParseConfigXML(configpath string) error {
	xmlFile, err := os.Open(configpath)
	if err != nil {
		return err
	}
	defer xmlFile.Close()

	var list ConfigOptionList
	err = xml.NewDecoder(xmlFile).Decode(&list)
	if err != nil {
		return err
	}

	for _, o := range list.ConfigOptions {
		switch o.Key {
		case "id":
			id = o.Value
		case "blockfileroot":
			root = o.Value
		case "serverhost":
			serverhost = o.Value
		case "serverport":
			serverport = o.Value
		case "sizeofblock":
			n, err := strconv.ParseInt(o.Value, 0, 64)
			if err != nil {
				return err
			}

			if n < int64(4096) {
				return errors.New("Buffer size must be greater than or equal to 4096 bytes")
			}
			SIZEOFBLOCK = n
		default:
			return errors.New("Bad ConfigOption received Key : " + o.Key + " Value : " + o.Value)
		}
	}

	return nil
}

func Run(configpath string) {

	ParseConfigXML(configpath)

	err := os.Chdir(root)
	CheckError(err)

	conn, err := net.Dial("tcp", serverhost+":"+serverport)
	CheckError(err)

	encoder := json.NewEncoder(conn)
	decoder := json.NewDecoder(conn)
	PacketChannel := make(chan Packet)
	// start communication
	go ReceivePackets(decoder, PacketChannel)
	tick := time.Tick(2 * time.Second)
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
