package main

import (
	"bufio"
	"bytes"
	"encoding/json"
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

var headerChannel chan blockheader
var root *filenode
var filemap map[string][]blockheader //TODO combine with Nodes
var datanodemap map[string]*datanode




var id string



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
	SRC	 	 string
	DST 	 string
	command  int
	data     block
	headers  []blockheader
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
	ID     		string
	mu     		sync.Mutex
	conn 	 	net.Conn
	sender      chan packet
	receiver	chan packet
	listed 		bool
}


func (dn *datanode) SendPacket(p packet) {
//	dn.mu.Lock()
	dn.sender <- p
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

// reads incoming block headers into filesystem
func HandleBlockHeaders() {
	for h := range headerChannel {
		MergeNode(h)
	}
}

// TODO duplicates(in upper level function)
// TODO a more logical structure for local vs remote pathing
func BlocksFromFile(localname, remotename string) []block {

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

	// Create blocks

	// TODO make this in parallel
	// TODO this will be headers in the future
	// TODO size bound
	numBlocks := int((info.Size() / SIZEOFBLOCK)) + 1
	blocks := make([]block, 0, numBlocks)

	// make a write buffer
	num := 0
	//  size := 0

	for {

		// read a chunk from file into block data buffer
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
		fmt.Println(n)

		// write full block to disc
		// TODO multiple blocks chosen

		h := blockheader{"DN1", remotename, n, num, numBlocks}
		data := make([]byte, 0, n)

		data = w.Bytes()[0:n]
		b := block{h, data}
		blocks = append(blocks, b)

		//TODO flush block to datanode?

		// generate new block
		num += 1

	}
	return blocks
}

// Run function dynamically to construct filesystem
//func MergeNode(master *Node, path string, block *Block)
//TODO duplicates
func MergeNode(h blockheader) {

	path := h.filename
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
				// If file already been added, we add the blockheader to the map
				if partial == path {
					filemap[path] = append(filemap[path], h)
					fmt.Println("adding header to filemap at " + path)
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
					filemap[path] = make([]blockheader, 0, 5)
					filemap[path][0] = h
					fmt.Println("creating blockheader at " + path)
				}

				n.parent = q
				q.children = append(q.children, n)
				q = n
			}
		}
	}
}

func DistributeBlocks(blocks []block) {
	for _, d := range blocks {
		//TODO sanity check
		dn, ok := datanodemap[d.header.datanodeID]
		if !ok {
			log.Printf("Error distributing block with datanodeID %s\n", d.header.datanodeID)
			continue
		}

		p := new(packet)
		p.DST = d.header.datanodeID
		p.SRC = id
		p.command = BLOCK
		p.data = d
		dn.SendPacket(*p)
	}
}

func EncodePacket(conn net.Conn, p packet) {
	encoder := json.NewEncoder(conn)
	err := encoder.Encode(p)
	if err != nil {
		log.Println("error sending", p.DST)
	}
}

func (dn *datanode) DecodePackets(conn net.Conn) {

	for {
		var p packet
		decoder := json.NewDecoder(conn)
		err := decoder.Decode(&p)
		if err != nil {
			log.Println("error receiving: err = ")
			return
		}

		if (p.SRC != "") {
			dn.receiver <- p
		}

		
		
	}
	
}



func  (dn *datanode) Handle(p packet) {
	fmt.Println("Handling packet")
	listed := dn.GetListed()

	r := packet
	r.SRC = id
	r.DST = p.SRC


	if p.command == HB {

		// check if block list has been retrieved already
		// TODO make periodic
		if !listed {
			r.command = LIST
		} else {
			r.command = ACK
		}

	} else if p.command == LIST {

		fmt.Printf("Listing directory contents from %s \n", p.SRC)
		list := p.headers
		//TODO block merging ?
		for _, b := range list {
			headerChannel <- b
		}
		//TODO conditional check(errors)
		//TODO syncronize any access to datanodes!
		dn.SetListed(true)

		r.command = ACK

	}
	// send response
	fmt.Println("sending", *r)
	EncodePacket(dn.conn,*r)
}

func CheckConnection(conn net.Conn, p packet){
	dn, ok := datanodemap[p.SRC]
	if !ok {
		fmt.Println("Adding new datanode :", p.SRC)
		datanodemap[p.SRC] = &datanode{p.SRC, sync.Mutex{}, conn, make(chan packet), make(chan packet), false}
	}else {
		fmt.Printf("Datanode %s reconnected \n", dn.ID)
		dn.conn = conn
	}
}


func (dn *datanode) HandleConnection(){

	go dn.DecodePackets(dn.conn)
	for r := range dn.receiver {
		fmt.Println("decoding packet ", r)
		dn.Handle(r)
	}
}

func HandleConnection(conn net.Conn) {

	// receive first packet and add datanode if necessary
	var p packet
	decoder := json.NewDecoder(conn)
	err := decoder.Decode(&p)
	fmt.Printf("ID %s Command %d \n", p.SRC, p.command)
	if err != nil {
		log.Println("error receiving")
	}
	CheckConnection(conn, p)

	// start datanode

	dn := datanodemap[p.SRC]
	fmt.Println("dn", *dn)
	dn.HandleConnection()
}

// interact with user
func ReceiveInput() {
	fmt.Println("Enter file to send")
	var localname string
	fmt.Scan(&localname)

	blocks := BlocksFromFile(localname, localname)
	DistributeBlocks(blocks)
}

func main() {

	// setup filesystem
	root = &filenode{"/", nil, make([]*filenode, 0, 5)}
	filemap = make(map[string][]blockheader)
	// go HandleBlockHeaders()

	// setup user interaction
	//go ReceiveInput()

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
