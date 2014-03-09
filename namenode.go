package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"net"
	"os"
	"strings"
	"sync"
)

const SERVERADDR = "localhost:8080"
const SIZEOFBLOCK = 1000

var headerChannel chan BlockHeader
var root *filenode
var filemap map[string](map[int][]BlockHeader) //TODO combine with Nodes
var datanodemap map[string]*datanode

var id string

const (
	HB       = iota
	LIST     = iota
	ACK      = iota
	BLOCK    = iota
	BLOCKACK = iota
)

// A file is composed of one or more Blocks
type Block struct {
	Header BlockHeader
	Data   []byte
}

// Block Headers hold Block Data information
type BlockHeader struct {
	DatanodeID string
	Filename   string //the Filename including the path "/data/test.txt"
	Size       uint64 //Size of Block in bytes
	BlockNum   int    // the 0 indexed position of Block within file
	NumBlocks  int    // total number of Blocks in file
}

// Packets are sent over the network
type Packet struct {
	SRC     string
	DST     string
	CMD     int
	Data    Block
	Headers []BlockHeader
}

// filenodes compose a tree representation of the filesystem
type filenode struct {
	path     string
	parent   *filenode
	children []*filenode
}

// Represent connected Datanodes
// Hold file and connection information
// Syncronized access to individual datanode contents
type datanode struct {
	ID       string
	mu       sync.Mutex
	conn     *net.Conn
	sender   chan Packet
	receiver chan Packet
	listed   bool
	size     uint64
}

func (dn *datanode) SendPacket(p Packet) {
	//	dn.mu.Lock()
	EncodePacket(*dn.conn, p)
	LogJSON(p)
	//	dn.mu.Unlock()
}

func (dn *datanode) SetListed(listed bool) {
	//	dn.mu.Lock()
	dn.listed = listed
	//	dn.mu.Unlock()
	return
}

func (dn *datanode) GetListed() bool {
	//	dn.mu.Lock()
	listed := dn.listed
	//	dn.mu.Unlock()
	return listed
}

func (dn *datanode) SetID(id string) {
	//	dn.mu.Lock()
	dn.ID = id
	//	dn.mu.Unlock()
	return
}

func (dn *datanode) GetID() string {
	//	dn.mu.Lock()
	id := dn.ID
	//	dn.mu.Unlock()
	return id
}

// reads incoming block Headers uint64o filesystem
func HandleBlockHeaders() {
	for h := range headerChannel {
		MergeNode(h)
	}
}

// TODO duplicates(in upper level function)
// TODO a more logical structure for local vs remote pathing
func BlocksFromFile(localname, remotename string) []Block {

	info, err := os.Lstat(localname)
	if err != nil {
		panic(err)
	}
	// TODO sanity checks
	//if (info == nil || info.Size == 0 ){
	//}

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
	// TODO make this in parallel
	// TODO this will be Headers in the future
	total := int((info.Size() / SIZEOFBLOCK) + 1)
	bls := make([]Block, 0, total)

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
		// TODO multiple Blocks chosen
		if strings.Index(remotename, "/") != 0 {
			remotename = "/" + remotename
		}

		//choose optimal datanode
		min_size := uint64(math.MaxUint64)
		var min_node *datanode
		for _, v := range datanodemap {
			if v.size < min_size {
				min_size = v.size
				min_node = v
			}
		}

		h := BlockHeader{min_node.ID, remotename, uint64(n), num, total}
		data := make([]byte, 0, n)

		data = w.Bytes()[0:n]
		b := Block{h, data}
		bls = append(bls, b)

		//TODO flush block to datanode?

		// generate new Block
		num += 1

	}
	return bls
}

func ContainsHeader(arr []BlockHeader, h BlockHeader) bool {
	for _, v := range arr {
		if h == v {
			return true
		}
	}
	return false
}

// Run function dynamically to construct filesystem
//TODO duplicates
func MergeNode(h BlockHeader) {

	path := h.Filename
	path_arr := strings.Split(path, "/")
	q := root

	for i, _ := range path_arr {
		//skip root level
		if i == 0 {
			continue
		} else {

			partial := strings.Join(path_arr[0:i+1], "/")
			exists := false
			for _, v := range q.children {
				if v.path == partial {
					q = v
					exists = true
					break
				}
			}
			if exists {
				// If file already been added, we add the BlockHeader to the map
				if partial == path {
					arr := filemap[path][h.BlockNum]
					if !ContainsHeader(arr, h) {
						filemap[path][h.BlockNum] = append(filemap[path][h.BlockNum], h)
						datanodemap[h.DatanodeID].size += h.Size
					}
					fmt.Println("adding Block header # ", h.BlockNum, "to filemap at ", path)
				}
				//else it is a directory
				continue
			} else {

				/*  if we are at file, create the map entry
				    TODO use Node for file_map rather than string
				    requires a lookup structure or function
				*/
				n := &filenode{partial, q, make([]*filenode, 0, 5)}
				if partial == path {
					filemap[path] = make(map[int][]BlockHeader)
					filemap[path][h.BlockNum] = make([]BlockHeader, 0, 5)
					filemap[path][h.BlockNum] = append(filemap[path][h.BlockNum], h)
					datanodemap[h.DatanodeID].size += h.Size
					fmt.Println("creating Block header # ", h.BlockNum, "to filemap at ", path)
				}

				n.parent = q
				q.children = append(q.children, n)
				q = n
			}
		}
	}
}

func DistributeBlocks(bls []Block) {
	fmt.Println("distributing ", len(bls), " bls")
	for _, d := range bls {
		//TODO sanity check
		dn, ok := datanodemap[d.Header.DatanodeID]
		if !ok {
			log.Printf("Error distributing Block with DatanodeID %s\n", d.Header.DatanodeID)
			continue
		}

		p := new(Packet)
		p.DST = d.Header.DatanodeID
		p.SRC = id
		p.CMD = BLOCK
		p.Data = Block{d.Header, d.Data}
		LogJSON(*p)

		dn.SendPacket(*p)
	}
}

func EncodePacket(conn net.Conn, p Packet) {
	encoder := json.NewEncoder(conn)
	err := encoder.Encode(p)
	if err != nil {
		log.Println("error sending", p.DST)
	}
}

func LogJSON(key interface{}) {
	outFile, err := os.Create("/home/sjarvie/logNN.json")
	CheckError(err)
	encoder := json.NewEncoder(outFile)
	err = encoder.Encode(key)
	CheckError(err)
	outFile.Close()
}

func (dn *datanode) DecodePackets(conn net.Conn) {

	for {
		var p Packet
		decoder := json.NewDecoder(conn)
		err := decoder.Decode(&p)
		if err != nil {
			log.Println("error receiving: err = ")
			return
		}

		if p.SRC != "" {
			dn.receiver <- p
		}

	}

}

func (dn *datanode) Handle(p Packet) {
	listed := dn.GetListed()

	r := Packet{id, p.SRC, 1, *new(Block), make([]BlockHeader, 0)}
	r.SRC = "NN"
	r.DST = p.SRC
	r.CMD = 1

	switch p.CMD {
	case HB:
		// TODO make periodic
		if !listed {
			r.CMD = LIST
		} else {
			r.CMD = ACK
		}

	case LIST:

		fmt.Printf("Listing directory contents from %s \n", p.SRC)
		list := p.Headers
		//TODO Block merging ?
		for _, h := range list {
			fmt.Println(h)
			headerChannel <- h
		}
		//TODO conditional check(errors)
		dn.SetListed(true)
		r.CMD = ACK

	case BLOCKACK:
		// receive acknowledgement for single Block header as being stored
		fmt.Println("Received BLOCKACK")
		if p.Headers != nil && len(p.Headers) == 1 {
			headerChannel <- p.Headers[0]
		}
		r.CMD = ACK
	}

	// send response

	EncodePacket(*dn.conn, r)
}

func CheckConnection(conn net.Conn, p Packet) {
	dn, ok := datanodemap[p.SRC]
	if !ok {
		fmt.Println("Adding new datanode :", p.SRC)
		datanodemap[p.SRC] = &datanode{p.SRC, sync.Mutex{}, &conn, make(chan Packet), make(chan Packet), false, 0}
	} else {
		fmt.Printf("Datanode %s reconnected \n", dn.ID)
		dn.conn = &conn
	}
}

func (dn *datanode) HandleConnection() {

	go dn.DecodePackets(*dn.conn)
	for r := range dn.receiver {
		dn.Handle(r)
	}
}

func HandleConnection(conn net.Conn) {

	// receive first Packet and add datanode if necessary
	var p Packet
	decoder := json.NewDecoder(conn)
	err := decoder.Decode(&p)
	if err != nil {
		log.Println("error receiving")
	}
	CheckConnection(conn, p)

	// start datanode

	dn := datanodemap[p.SRC]
	dn.HandleConnection()
}

// uint64eract with user
func ReceiveInput() {
	for {
		fmt.Println("Enter command to send")

		var cmd string
		var localname string
		fmt.Scan(&cmd)
		fmt.Scan(&localname)
		
		if !(cmd == "put" || cmd == "get") {
			fmt.Printf("Incorrect command\n Valid Commands: \n \t put [filename] \n \t get [filename] \n")
			continue
		}

		_, err := os.Lstat(localname)
		if err != nil {
			fmt.Println("Invalid File")
			continue
		}

		if cmd == "put" {
			bls := BlocksFromFile(localname, localname)
			DistributeBlocks(bls)
		} else {
			fmt.Printf("Retrieving file")
		}

	}
}

func main() {

	// setup filesystem
	root = &filenode{"/", nil, make([]*filenode, 0, 5)}
	filemap = make(map[string]map[int][]BlockHeader)
	headerChannel = make(chan BlockHeader)

	go HandleBlockHeaders()

	// setup user uint64eraction
	go ReceiveInput()

	id = "NN"
	listener, err := net.Listen("tcp", SERVERADDR)
	CheckError(err)

	datanodemap = make(map[string]*datanode)

	// listen for datanode connections
	for {
		conn, err := listener.Accept()
		CheckError(err)
		go HandleConnection(conn)
	}
}

func CheckError(err error) {
	if err != nil {
		log.Fatal("Fatal error ", err.Error())
	}
}
