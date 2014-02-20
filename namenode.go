
/*
The namenode listens for user or data node connections
*/

package main

import (
    "fmt"
    "log"
    "net"
    "encoding/gob"
)

const listenAddr = "localhost:8080"

type Packet struct {
    command, data string
}
func handleConnection(conn net.Conn) {
    fmt.Println("Connected to a node")

    dec := gob.NewDecoder(conn)
    p := &Packet{}
    dec.Decode(p)
    fmt.Printf("Received : %+v", p);
}

func main() {
    fmt.Println("start");
   ln, err := net.Listen("tcp", listenAddr)
    if err != nil {
        log.Fatal(err)
    }
    for {
        conn, err := ln.Accept() // this blocks until connection or error
        if err != nil {
            log.Fatal(err)
        }
        go handleConnection(conn) // a goroutine handles conn so that the loop can accept other connections
    }
}