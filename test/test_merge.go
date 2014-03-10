package godfstest

import (
    "strings"
    "fmt"
)


type Node struct {
    Path string
    Parent *Node
    Children []*Node
}


//func mergeNode(master *Node, path string, block *Block)
func mergeNode(master *Node, path string) {
    add_path := strings.Split(path, "/")
    q := master

    for i,_ := range add_path {
        //skip root
        if i == 0 {
            continue
        } else {

            path_partial := strings.Join(add_path[0:i+1],"/")
            exists := false
            for _,v := range q.Children {
                //fmt.Println(*v)
                if v.Path == path_partial {
                    q = v
                    exists = true
                    break
                }
            }
            if exists {
                continue
            } else {
                n := &Node{path_partial,q,make([] *Node, 0, 5)}

                n.Parent = q
                q.Children = append(q.Children,n)
                fmt.Println(q.Children)

                q = n
            }
        }
    }

}





func main() {
    root := Node {"/",nil, make([]*Node, 0, 5)}
    //datanode := Node {"/d1/t1.txt",nil, make([]*Node, 5)}

    mergeNode(&root, "/d1/t1.txt")
    mergeNode(&root, "/d1/t2.txt")
    mergeNode(&root, "/d2/u2/t3.txt")
    fmt.Println(root.Children[1].Children[0])
    
}

