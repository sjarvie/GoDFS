package godfstest

import (
    "fmt"
    "sync"
    "time"
)


 type datanode struct {
    id string
    mu  sync.Mutex
    listed bool
    count int
}


func (dn *datanode)SetEven(){
    for {
        dn.mu.Lock()
        dn.count = 2
        dn.mu.Unlock()
    }
}


func (dn *datanode)SetOdd(){
    for {
        dn.mu.Lock()
        dn.count = 1
        dn.mu.Unlock()
    }
}

func main() {
    dn := datanode{"1", sync.Mutex{}, false, 1}

    go dn.SetEven()
    go dn.SetOdd()

    tick := time.Tick(1000 * time.Millisecond)
     
    for t := range tick {
        fmt.Println(dn.count, "       ", t)

    }


}