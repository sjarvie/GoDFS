package godfstest

import (
 "fmt"
// "net"
// "encoding/gob"
// "log"
// "strings"
 "os"
 "io"
 "bufio"
 "bytes"
)

const SIZEOFBLOCK = 10

type datablock struct {
    header blockheader
    data []byte
}

type blockheader struct {
    datanodeID string
    filename string 
    size, sequenceNum, numBlocks int 
}

// TODO duplicates(in upper level function)
// TODO a more logical structure for local vs remote pathing
func BlocksFromFile(localname, remotename string) []datablock {

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
    blocks := make([]datablock, 0, numBlocks)
    
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
        b := datablock{h, data}
        blocks = append(blocks, b)

        //TODO flush block to datanode?

        // generate new block
        num += 1

    }
    return blocks
}

func main() {
    localname := os.Args[1]

    blocks := BlocksFromFile(localname, localname)
    for _,f := range blocks {
       // d := f.data
        fmt.Println(f)
        //for _,c := range d {
         //   fmt.Printf(string(c))
        //}
        
    }

}