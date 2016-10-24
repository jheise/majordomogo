package main

import (
	// standard
	"fmt"
	"time"

	// external
	zmq "github.com/pebbe/zmq4"
)

func sendReady(soc *zmq.Socket) {
	frame0 := []byte("")
	frame1 := []byte("MDPW01")
	frame2 := []byte("1")
	frame3 := []byte("basic")
	data := [][]byte{frame0, frame1, frame2, frame3}

	soc.SendMessage(data, 0)
}

func main() {
	context, err := zmq.NewContext()
	if err != nil {
		panic(err)
	}
	defer context.Term()

	worker, err := context.NewSocket(zmq.DEALER)
	if err != nil {
		panic(err)
	}
	defer worker.Close()
	ident := fmt.Sprintf("worker%d", time.Now().Unix())
	worker.SetIdentity(ident)
	worker.Connect("tcp://localhost:9999")

	sendReady(worker)

	total := 0
	for {
		worker.SendMessage([][]byte{[]byte(""), []byte("HELLO")}, 0)

		parts, _ := worker.RecvMessageBytes(0)
		workload := parts[1]
		fmt.Println("Workload: " + string(workload))

		if string(workload) == "FIRED" {
			id, _ := worker.GetIdentity()
			fmt.Printf("Complete: %d tasks (%s)\n", total, id)
			break
		}
		total++

		time.Sleep(3 * time.Second)
	}
}
