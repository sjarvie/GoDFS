
package main
import (
    "fmt"
    "net"
    "os"
    "encoding/gob"
    "time"
    "io/ioutil"
)

const SERVERADDR = "localhost:8080"
const SIZEOFBLOCK = 1000000
var id, blockpath string // command line arguments
var state = "HB"



// A file is composed of one or more blocks
type datablock struct {
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
    from, to string
    cmd string
    data block
    headers []blockheader
}

// receive a packet and send to syncronized channel
func ReceivePacket(conn net.Conn, p chan Packet){
    decoder := gob.NewDecoder(conn)
    for {
        var r Packet
        decoder.Decode(&r)
        p <- r
    }
}

func (p *packet) String() string {
    s := "NN" + p.from
    return s
}

// change state to handle request from server
func HandleResponse(r Packet){
    if r.cmd == "ACK" {
        state = "HB"
    } else if r.cmd == "LIST" {
        state = "LIST"
        fmt.Println("state now " + state)
    }
}

// handle a state's action
func PerformAction(encoder *gob.Encoder){
    p := new(Packet);
    p.from = id
    p.to = "NN" //TODO better naming conventions
    if state == "HB" {
        p.cmd = "HB"
    } else if state == "LIST"{
        list := GetBlockInfos()
        p.headers = make([]blockheader, len(list))

        for _, b := range list {
            apend(p.headers,b)
        }
        p.cmd = "LIST"
    }
    encoder.Encode(p) 
}

//func blocksFromFile

// List block contents
// Later add recursion and switch to blocks
func GetBlockHeaders() []blockheader {

    list, err := ioutil.ReadDir(blockpath)
    CheckError(err)
    headers := make([]blockheader, len())

    for f in range list {
        var b block
        ReadGob(f.Name, &b)
        append(headers, b)
    }
    return headers
}


func ReadGob(fileName string, key interface{}) {
    inFile, err := os.Open(fileName)
    CheckError(err)
    decoder := gob.NewDecoder(inFile)
    err = decoder.Decode(key)
    CheckError(err)
    inFile.Close()
}

func WriteGob(fileName string, key interface{}) {
    outFile, err := os.Create(fileName)
    CheckError(err)
    encoder := gob.NewEncoder(outFile)
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

    encoder := gob.NewEncoder(conn)

    packetChannel := make(chan Packet)
    tick := time.Tick(1000 * time.Millisecond)

    go ReceivePacket(conn,packet_channel)

    for {
        select {
            case <-tick:
                PerformAction(encoder)
                fmt.Println("state : " + state)
            case r := <-packetChannel:
                PerformAction(r)
                fmt.Println("Received : " + r.String())
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
