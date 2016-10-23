package main

import (
	// standard
	"fmt"
	"time"

	// external
	"github.com/alecthomas/gozmq"
)

func main() {
	context, err := gozmq.NewContext()
	if err != nil {
		panic(err)
	}
	defer context.Close()

	worker, err := context.NewSocket(gozmq.DEALER)
	if err != nil {
		panic(err)
	}
	defer worker.Close()
	ident := fmt.Sprintf("worker%d", time.Now().Unix())
	worker.SetIdentity(ident)
	worker.Connect("tcp://localhost:9999")

	total := 0
	for {
		worker.SendMultipart([][]byte{[]byte(""), []byte("HELLO")}, 0)

		parts, _ := worker.RecvMultipart(0)
		workload := parts[1]
		fmt.Println("Workload: " + string(workload))

		if string(workload) == "FIRED" {
			id, _ := worker.Identity()
			fmt.Printf("Complete: %d tasks (%s)\n", total, id)
			break
		}
		total++

		time.Sleep(3 * time.Second)
	}
}
