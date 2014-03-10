package namenode

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"sort"
	"strings"
	"sync"
)

const SERVERADDR = "localhost:8080"
const SIZEOFBLOCK = 1000

var headerChannel chan BlockHeader
var sendChannel chan Packet
var sendMap map[string]*json.Encoder // maps DatanodeIDs to their connections
var sendMapLock sync.Mutex
var blockReceiverChannel chan Block // used to fetch blocks on user request
var blockRequestorChannel chan BlockHeader

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
	GETBLOCK = iota
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
	ID     string
	mu     sync.Mutex
	listed bool
	size   uint64
}

type By func(p1, p2 *datanode) bool

func (by By) Sort(nodes []datanode) {
	s := &datanodeSorter{
		nodes: nodes,
		by:    by, // The Sort method's receiver is the function (closure) that defines the sort order.
	}
	sort.Sort(s)
}

type datanodeSorter struct {
	nodes []datanode
	by    func(p1, p2 *datanode) bool // Closure used in the Less method.
}

func (s *datanodeSorter) Len() int {
	return len(s.nodes)
}
func (s *datanodeSorter) Swap(i, j int) {
	s.nodes[i], s.nodes[j] = s.nodes[j], s.nodes[i]
}

// Less is part of sort.Interface. It is implemented by calling the "by" closure in the sorter.
func (s *datanodeSorter) Less(i, j int) bool {
	return s.by(&s.nodes[i], &s.nodes[j])
}

func (dn *datanode) SendPacket(p Packet) {
	//	dn.mu.Lock()
	sendChannel <- p
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

	var bls []Block
	if len(datanodemap) < 1 {
		fmt.Println("No connected datanodes, cannot save file")
		return bls
	}

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
	bls = make([]Block, 0, total)

	num := 0

	//Setup nodes for block load balancing

	sizeArr := make([]datanode, len(datanodemap))
	i := 0
	for _, v := range datanodemap {
		sizeArr[i] = *v
		i++
	}
	bysize := func(dn1, dn2 *datanode) bool {
		return dn1.size < dn2.size
	}
	By(bysize).Sort(sizeArr)
	nodeindex := 0

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

		h := BlockHeader{sizeArr[nodeindex].ID, remotename, uint64(n), num, total}

		// load balance via roundrobin
		nodeindex = (nodeindex + 1) % len(sizeArr)

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
				n := &filenode{partial, q, make([]*filenode, 5, 5)}
				if partial == path {
					filemap[path] = make(map[int][]BlockHeader)
					filemap[path][h.BlockNum] = make([]BlockHeader, 5, 5)
					filemap[path][h.BlockNum][0] = h
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
		_, ok := datanodemap[d.Header.DatanodeID]
		if !ok {
			log.Printf("Error distributing Block with DatanodeID %s\n", d.Header.DatanodeID)
			continue
		}

		p := new(Packet)
		p.DST = d.Header.DatanodeID
		p.SRC = id
		p.CMD = BLOCK
		p.Data = Block{d.Header, d.Data}

		sendChannel <- *p
	}
}

func SendPackets() {
	for p := range sendChannel {
		sendMapLock.Lock()
		encoder := sendMap[p.DST]
		err := encoder.Encode(p)
		if err != nil {
			log.Println("error sending", p.DST)
		}
		sendMapLock.Unlock()
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

func (dn *datanode) Handle(p Packet) {
	listed := dn.GetListed()

	r := Packet{id, p.SRC, ACK, *new(Block), make([]BlockHeader, 0)}

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

	case BLOCK:
		blockReceiverChannel <- p.Data
		r.CMD = ACK
	}

	// send response
	sendChannel <- r

}

func CheckConnection(conn net.Conn, p Packet) {
	dn, ok := datanodemap[p.SRC]
	if !ok {
		fmt.Println("Adding new datanode :", p.SRC)
		datanodemap[p.SRC] = &datanode{p.SRC, sync.Mutex{}, false, 0}
	} else {
		fmt.Printf("Datanode %s reconnected \n", dn.ID)
	}
	sendMapLock.Lock()
	sendMap[p.SRC] = json.NewEncoder(conn)
	sendMapLock.Unlock()
	dn = datanodemap[p.SRC]
	dn.Handle(p)
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
	for {
		var p Packet
		err := decoder.Decode(&p)
		if err != nil {
			log.Println("error receiving: err = ")
			return
		}
		dn.Handle(p)
	}
}

func ConstructFile(localname, remotename string, headers []BlockHeader) {
	outFile, err := os.Create(localname)
	CheckError(err)
	w := bufio.NewWriter(outFile)

	// send a request packet and wait for response block before fetching next block
	for _, h := range headers {

		p := new(Packet)
		p.SRC = id
		p.DST = h.DatanodeID
		p.CMD = GETBLOCK
		p.Headers = []BlockHeader{h}
		fmt.Println("Adding to sendchannel ", *p)
		sendChannel <- *p
		b := <-blockReceiverChannel
		if b.Header == h {
			if _, err := w.Write(b.Data[:b.Header.Size]); err != nil {
				panic(err)
			}
		}
		fmt.Println("Printed block ", b)

	}
	w.Flush()
	outFile.Close()

}

// uint64eract with user
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
				fmt.Println("Invalid File")
				continue
			}
			bls := BlocksFromFile(localname, remotename)
			DistributeBlocks(bls)
		} else {
			remotename := file1
			localname := file2

			fmt.Println("Retrieving file")
			headernumbers, ok := filemap[remotename]
			if !ok || len(headernumbers) == 0 {
				fmt.Println("Remote file not found in system")
				continue
			}

			num := headernumbers[0][0].NumBlocks
			fmt.Println("NumBlocks ", num)
			//TODO ensure correct blocks
			if len(headernumbers) != num {
				fmt.Println("Could not find all blocks")
			}

			fetchHeaders := make([]BlockHeader, num, num)

			i := 0
			for i < num {

				headers := filemap[remotename][i]
				old_i := i

				for _, h := range headers {
					fmt.Println(h)
					if h.BlockNum == i {

						// TODO this can break if multiple calls are made
						fetchHeaders[i] = h
						fmt.Println("adding request for header ", h)
						i++
						break
					}
				}
				if i == old_i {
					fmt.Println("Could not find blockNum ", i)
					return
				}

			}

			fmt.Println("ConstructFile")

			ConstructFile(localname, remotename, fetchHeaders)

		}

	}
}

func Init() {

	// setup filesystem
	root = &filenode{"/", nil, make([]*filenode, 0, 5)}
	filemap = make(map[string]map[int][]BlockHeader)
	headerChannel = make(chan BlockHeader)
	blockReceiverChannel = make(chan Block) // used to fetch blocks on user request

	sendChannel = make(chan Packet)
	sendMap = make(map[string]*json.Encoder)
	sendMapLock = sync.Mutex{}

	go HandleBlockHeaders()

	// setup user uint64eraction
	go ReceiveInput()
	go SendPackets()

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
