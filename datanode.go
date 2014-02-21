/* Gob EchoClient
 */
package main
import (
    "fmt"
    "net"
    "os"
    "encoding/gob"
    "time"
)

const serverAddr = "localhost:8080"
var id string

type Packet struct {
    ID string
    Command string
    Data string
}

func (p Packet) String() string {
    s := "DN" + p.ID
    return s
}


func main() {
    if len(os.Args) != 2 {
        fmt.Println("Usage: ", os.Args[0], "id")
        os.Exit(1)
    }
    id = os.Args[1]

    heartbeat := Packet{ID: id, Command : "HB", Data: ""}
    conn, err := net.Dial("tcp", serverAddr)
    checkError(err)

    encoder := gob.NewEncoder(conn)
    decoder := gob.NewDecoder(conn)

    for {
        time.Sleep(1000 * time.Millisecond)
        encoder.Encode(heartbeat)
        var responsePacket Packet
        decoder.Decode(&responsePacket)
        fmt.Println(responsePacket.String())
     }
     os.Exit(0)
}

func checkError(err error) {
    if err != nil {
        fmt.Println("Fatal error ", err.Error())
        os.Exit(1)
    }
}
