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

func main() {
    fmt.Println("start client");
    conn, err := net.Dial("tcp", listenAddr)
    if err != nil {
        log.Fatal(err)
    }
    encoder := gob.NewEncoder(conn)
    p := &Packet{"command", "data"}
    encoder.Encode(p)
    conn.Close()
    fmt.Println("done");
}