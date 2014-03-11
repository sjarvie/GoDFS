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

var headerChannel chan BlockHeader   // processes headers into filesystem
var sendChannel chan Packet          //  enqueued packets for transmission
var sendMap map[string]*json.Encoder // maps DatanodeIDs to their connections
var sendMapLock sync.Mutex
var blockReceiverChannel chan Block        // used to fetch blocks on user request
var blockRequestorChannel chan BlockHeader // used to send block requests

var root *filenode                             // the filesystem
var filemap map[string](map[int][]BlockHeader) // filenames to blocknumbers to headers
var datanodemap map[string]*datanode           // filenames to datanodes

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
	Header BlockHeader // metadata
	Data   []byte      // data contents
}

// Blockheaders hold Block metadata
type BlockHeader struct {
	DatanodeID string // ID of datanode which holds the block
	Filename   string //the remote name of the block including the path "/test/0"
	Size       uint64  // size of Block in bytes
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

// filenodes compose an internal tree representation of the filesystem
type filenode struct {
	path     string
	parent   *filenode
	children []*filenode
}

// Represent connected Datanodes
// Hold file and connection information
type datanode struct {
	ID     string
	listed bool
	size   uint64
}

// By is used to select the fields used when comparing datanodes
type By func(p1, p2 *datanode) bool

// Sort sorts an array of datanodes by the function By
func (by By) Sort(nodes []datanode) {
	s := &datanodeSorter{
		nodes: nodes,
		by:    by, // The Sort method's receiver is the function (closure) that defines the sort order.
	}
	sort.Sort(s)
}

// structure to sort datanodes
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

// Sendpacket abstracts packet sending details
func (dn *datanode) SendPacket(p Packet) {
	sendChannel <- p
}

// HandleBlockHeaders reads incoming Blockheaders and merges them into the filesystem
func HandleBlockHeaders() {
	for h := range headerChannel {
		MergeNode(h)
	}
}

// BlocksFromFile split a File into Blocks for storage on the filesystem
// in the future this will return only headers to the client, which will generate Blocks
// based on header information and distribute the files to the proper nodes
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

		// generate new Block
		num += 1

	}
	return bls
}

// ContainsHeader searches a Blockheader for a given Blockheader
func ContainsHeader(arr []BlockHeader, h BlockHeader) bool {
	for _, v := range arr {
		if h == v {
			return true
		}
	}
	return false
}

// Mergenode adds a Blockheader entry to the filesystem, in its correct location
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
					//fmt.Println("adding Block header # ", h.BlockNum, "to filemap at ", path)
				}
				//else it is a directory
				continue
			} else {

				//  if we are at file, create the map entry
				n := &filenode{partial, q, make([]*filenode, 5, 5)}
				if partial == path {
					filemap[path] = make(map[int][]BlockHeader)
					filemap[path][h.BlockNum] = make([]BlockHeader, 5, 5)
					filemap[path][h.BlockNum][0] = h
					datanodemap[h.DatanodeID].size += h.Size
					//fmt.Log("creating Block header # ", h.BlockNum, "to filemap at ", path)
				}

				n.parent = q
				q.children = append(q.children, n)
				q = n
			}
		}
	}
}

// DistributeBlocks creates packets based on Blockheader metadata and enqueues them for transmission
func DistributeBlocks(bls []Block) {
	fmt.Println("distributing ", len(bls), " bls")
	for _, d := range bls {
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

// SendPackets encodes packets and transmits them to their proper recipients
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

// Handle handles a packet and performs the proper action based on its contents
func (dn *datanode) Handle(p Packet) {
	listed := dn.listed

	r := Packet{id, p.SRC, ACK, *new(Block), make([]BlockHeader, 0)}

	switch p.CMD {
	case HB:
		if !listed {
			r.CMD = LIST
		} else {
			r.CMD = ACK
		}

	case LIST:

		fmt.Printf("Listing directory contents from %s \n", p.SRC)
		list := p.Headers
		for _, h := range list {
			fmt.Println(h)
			headerChannel <- h
		}
		dn.listed = true
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

// Checkconnection adds or updates a connection to the namenode and handles its first packet
func CheckConnection(conn net.Conn, p Packet) {
	dn, ok := datanodemap[p.SRC]
	if !ok {
		fmt.Println("Adding new datanode :", p.SRC)
		datanodemap[p.SRC] = &datanode{p.SRC, false, 0}
	} else {
		fmt.Printf("Datanode %s reconnected \n", dn.ID)
	}
	sendMapLock.Lock()
	sendMap[p.SRC] = json.NewEncoder(conn)
	sendMapLock.Unlock()
	dn = datanodemap[p.SRC]
	dn.Handle(p)
}

// Handle Connetion initializes the connection and performs packet retrieval
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

// ConstructFile creates and writes a file to the local disc, fetching Blocks
// from datanodes in a serial fashion. This will be performed in parallel in the future
func ConstructFile(localname, remotename string, headers []BlockHeader) {
	outFile, err := os.Create(localname)
	if err != nil {
			log.Println("error constructing file: err = ")
			return
	}
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

// Init initializes all internal structures to run a namnode and handles incoming connections
func Init() {

	// setup filesystem
	root = &filenode{"/", nil, make([]*filenode, 0, 5)}
	filemap = make(map[string]map[int][]BlockHeader)

	// setup communication
	headerChannel = make(chan BlockHeader)
	blockReceiverChannel = make(chan Block)
	sendChannel = make(chan Packet)
	sendMap = make(map[string]*json.Encoder)
	sendMapLock = sync.Mutex{}
	go HandleBlockHeaders()
	go SendPackets()

	// setup user interaction
	go ReceiveInput()

	id = "NN"
	listener, err := net.Listen("tcp", SERVERADDR)
	if err != nil {
		log.Fatal("Fatal error ", err.Error())
	}

	datanodemap = make(map[string]*datanode)

	// listen for datanode connections
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Fatal("Fatal error ", err.Error())
		}
		go HandleConnection(conn)
	}
}
