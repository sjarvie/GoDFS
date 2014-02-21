package main

import (
 "fmt"
 "net"
 "encoding/gob"
 "log"
)

const serverAddr = "localhost:8080"

type Packet struct {
    ID string
    Command string
    Data string
}

func (p Packet) String() string {
    s := "NN" + p.ID
    return s
}

func handleConnection(conn net.Conn){
     encoder := gob.NewEncoder(conn)
     decoder := gob.NewDecoder(conn)
     for  {
         var packet Packet
         decoder.Decode(&packet)

         if (packet.Command == "HB"){
            fmt.Println("Received Heartbeat from %s", packet.ID)
            ackPacket := Packet {ID: "NN",Command : "ACK", Data: ""}
            encoder.Encode(ackPacket)
         }
     }
     conn.Close() // we're finished
}


func main() {

    listener, err := net.Listen("tcp", serverAddr)
    checkError(err)
    for {
        conn, err := listener.Accept()
        checkError(err)

        go handleConnection(conn)
        
        
     }
}

func checkError(err error) {
     if err != nil {
        log.Fatal("Fatal error ", err.Error())
     }
}
