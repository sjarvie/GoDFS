/* Gob EchoClient
 */
package main
import (
    "fmt"
    "net"
    "os"
    "encoding/gob"
    "time"
    "io/ioutil"
)

const serverAddr = "localhost:8080"
var id, path string // command line arguments
var action = "HB"

//Action that the data node is currently undergoing
// HB : Heartbeat : Waiting for a command
// LIST : Sending directory contents



type Packet struct {
    ID string
    Command string
    Data string
    Files [] string

}

func (p Packet) String() string {
    s := p.ID + " : " + p.Command
    return s
}


func recv(conn net.Conn, packet_channel chan Packet){
    decoder := gob.NewDecoder(conn)
    for {
        var responsePacket Packet
        decoder.Decode(&responsePacket)
        packet_channel <- responsePacket
    }
}



func handleResponse(response Packet){
    if response.Command == "ACK" {
        action = "HB"
    } else if response.Command == "LIST" {
        action = "LIST"
        fmt.Println("action now " + action)
    }
}

func performAction(encoder *gob.Encoder){
    p := new(Packet);
    p.ID = id
    if action == "HB" {
        p.Command = "HB"
    } else if action == "LIST"{
        files := listFS()
        p.Files = make([]string, len(files))
        for i, f := range files {
                fmt.Println(f.Name(), "\t", f.Size())
                p.Files[i] = f.Name()
        }
        p.Command = "LIST"
    }
    encoder.Encode(p) // TODO syncronize this call
}


// List directory contents
// Later add recursion and switch to blocks
func listFS() []os.FileInfo {
    files, _ := ioutil.ReadDir(path)
    return files

    //for _, f := range files {
    //        fmt.Println(f.Name())
    //}
}




func main() {
    if len(os.Args) != 3 {
        fmt.Println("Usage: ", os.Args[0], "id path")
        os.Exit(1)
    }
    id = os.Args[1]
    path = os.Args[2]

    conn, err := net.Dial("tcp", serverAddr)
    checkError(err)

    encoder := gob.NewEncoder(conn)

    packet_channel := make(chan Packet)
    tick := time.Tick(1000 * time.Millisecond)

    go recv(conn,packet_channel)

    for {
        select {
            case <-tick:
                performAction(encoder)
                fmt.Println("action : " + action)
            case response := <-packet_channel:
                handleResponse(response)
                fmt.Println("Received : " + response.String())
        }
                
     }
     os.Exit(0)
}

func checkError(err error) {
    if err != nil {
        fmt.Println("Fatal error ", err.Error())
        os.Exit(1)
    }
}
