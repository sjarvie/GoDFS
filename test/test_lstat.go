package godfstest

import (
	"os"
	"fmt"
)

type uinput struct {
	cmd string
	fname string
}


// interact with user
func ReceiveInput() {
	fmt.Println("Enter file to send")
	var localname uinput
	fmt.Scan(&localname.cmd)
	fmt.Scan(&localname.fname)


	st := BlocksFromFile(localname.fname)
	fmt.Println(st)
}

// TODO duplicates(in upper level function)
// TODO a more logical structure for local vs remote pathing
func BlocksFromFile(localname string)string {

	d,_ := os.Getwd()
	fmt.Println(d)

	_, err := os.Lstat(localname)
	if err != nil {
		panic(err)
	}
	// TODO sanity checks
	//if (info == nil || info.Size == 0 ){
	//}

	// get read buffer
	fi, err := os.Open(localname)
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := fi.Close(); err != nil {
			panic(err)
		}
	}()


	return fi.Name()
}


func main() {
	ReceiveInput()
}