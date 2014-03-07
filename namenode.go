package main
import (
 "fmt"
 "net"
 "encoding/gob"
 "log"
 "strings"
 "os"
 "bufio"
 "bytes"
 "io"
 "sync"
)

const SERVERADDR = "localhost:8080"
const SIZEOFBLOCK = 1000 

var headerChannel chan blockheader
var root *filenode
var filemap map[string][]blockheader  //TODO combine with Nodes


    
var id string

// A file is composed of one or more blocks
type block struct {
    header blockheader
    data []byte
}

// block headers hold block data information
type blockheader struct {
    datanodeID string
    filename string //the filename including the path "/data/test.txt"
    size, sequenceNum, numBlocks int  //size in bytes, the order of the chunk within file
    //TODO checksum
}

// packets are sent over the network
type packet struct {
    from,to string
    cmd string
    data block
    headers []blockheader
}

// filenodes compose a tree representation of the filesystem
type filenode struct {
    path string
    parent *filenode
    children []*filenode
}

// Represent connected Datanodes
// Hold file and connection information
// Syncronized access to individual datanode contents
type datanode struct {
    id string
    mu  sync.Mutex
    conn net.Conn
    listed bool
}


func (dn *datanode) SetListed(listed bool) {
    dn.mu.Lock()
    dn.listed = listed 
    dn.mu.Unlock()
}

func (dn *datanode) GetListed() (listed bool) {
    dn.mu.Lock()
    listed = dn.listed 
    dn.mu.Unlock()
    return
}

func (dn *datanode) SetID(id bool) {
    dn.mu.Lock()
    dn.id = listed 
    dn.mu.Unlock()
}

func (dn *datanode) GetID() (id bool) {
    dn.mu.Lock()
    id = dn.id 
    dn.mu.Unlock()
    return
}

func (dn *datanode) SendPacket(Packet p) {
    dn.mu.Lock()
    encoder := gob.NewEncoder(dn.conn)
    encoder.Encode(p)
    dn.mu.Unlock()
}

// TODO merge this into HandleConnection
func (dn *datanode) ReceivePacket() (p packet){
    dn.mu.Lock()
    var p packet
    decoder := gob.NewDecoder(dn.conn)
    decoder.Decode(&p)
    dn.mu.Unlock()
    return
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

    info,err := os.Lstat(localname)
    if err != nil {panic(err)}
    // TODO sanity checks
    //if (info == nil || info.Size == 0 ){
    //}


    // get read buffer
    fi, err := os.Open(localname)
    if err != nil { panic(err) }
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
        if err != nil && err != io.EOF { panic(err) }
        if n == 0 { break }
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

    for i,_ := range path_arr {
        //skip root level
        if i == 0 {
            continue
        } else {

            partial := strings.Join(path_arr[0:i+1],"/")
            exists := false
            for _,v := range q.children {
                if v.path == partial {
                    q = v
                    exists = true
                    break
                }
            }
            if exists {
                // If file already been added, we add the blockheader to the map
                if (partial == path){
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
                if (partial == path){
                    filemap[path] = make([]blockheader, 0 , 5)
                    filemap[path][0] = h
                    fmt.Println("creating blockheader at " + path)
                }

                n.parent = q
                q.children = append(q.children,n)
                q = n
            }
        }
    }
}


func DistributeBlocks(blocks []block){
    for _, d := range blocks{
        p := new(packet)
        //TODO sanity check
        p.to = d.header.datanodeID
        p.from = id
        p.cmd = "BLOCK"
        p.data = d
        p.Send()
    }
}


func (p *packet) Send() {
    dn, ok := datanodemap[p.from]
    if !ok {
        fmt.Println("Recipient not found :", p.from)
        return
    }
    dn.SendPacket(p)
}

func (p packet) Handle() {

    //TODO on reconnect or disconnect handle situation
    // TODO check for connection 
    

    r := new(packet)
    r.from = id
    r.to = p.from

    //acquire datanode
    dn, ok := datanodemap[p.from]
    if !ok {
        fmt.Println("Adding new datanode :", p.from)
        return
    }
    listed = dn.GetListed()

    if p.cmd == "HB" {

        // check if block list has been retrieved already
        // TODO make periodic
        if !listed{
            r.cmd = "LIST"
        } else {
            r.cmd = "ACK"
        }



     } else if p.cmd == "LIST" {

        fmt.Printf("Listing directory contents from %s \n", p.from)
        list := p.headers
        //TODO block merging ?
        for _, b := range list {
            headerChannel <- b
        }
        //TODO conditional check(errors)
        //TODO syncronize any access to datanodes!
        dn.SetListed(true)

        r.cmd = "ACK"
      
    }

    // send response
    r.Send()
    

}

func HandleConnection(conn net.Conn) { 

    

    // TODO make function
    // receive first packet and add datanode
    if err != nil {
       return
    }
    // TODO make fault tolerant in case of reconnect
    datanodemap[p.from] = datanode {p.from, sync.Mutex{}, conn, false}
    p.Handle()

    //Receive Packets
    for {
        var p Packet
        decoder := gob.NewDecoder(conn)
        err := decoder.Decode(&p)
        p.Handle()

    }
    //TODO kill datanode? or mark inactive
    conn.Close() 
}


// interact with user
func ReceiveInput() {
    fmt.Println("Enter file to send")
    var s string
    fmt.Scan(&s)

    blocks := BlocksFromFile(localname, localname)
    DistributeBlocks(blocks)
}


func main() {

    id = "NN"
    listener, err := net.Listen("tcp", SERVERADDR)
    CheckError(err)

    datanodemap = make(map[string]datanode)

    // setup filesystem
    root = &filenode{"/",nil, make([]*filenode, 0, 5)}
    filemap = make(map[string][]blockheader)
    go HandleBlockHeaders()

    // setup user interaction
    go ReceiveInput()
    
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
