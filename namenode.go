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
    Files [] string
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
         fmt.Printf("Received Packet from : %s command : %s \n", packet.ID, packet.Command)

         if packet.Command == "HB" {

            listPacket := new(Packet)
            listPacket.ID = "NN"
            listPacket.Command = "LIST"
            encoder.Encode(listPacket)
         } else if packet.Command == "LIST" {
            fmt.Printf("Listing directory contents from %s \n", packet.ID)
            files := packet.Files
            for _, f := range files {
                fmt.Println(f)
            }

            ackPacket := new(Packet)
            ackPacket.ID = "NN"
            ackPacket.Command = "ACK"
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
