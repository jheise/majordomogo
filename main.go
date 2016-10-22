package main

import (
	// standard
	"fmt"
	"time"

	// external
	"github.com/alecthomas/gozmq"
)

func worker_task() {
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
	worker.SetIdentify(fmt.Sprintf("worker%d", time.Now().Unix()))
	worker.Connect("tcp://localhost:9999")

	total := 0
	for {
		worker.SendMultipart([][]byte{[]byte(""), []byte("HELLO")}, 0)

		parts, nil := worker.RecvMultipart(0)
		workload := parts[1]

		if string(workload) == "FIRED" {
			id, err := worker.Identity()
			fmt.Printf("Complete: %d tasks (%s)\n", total, id)
			break
		}
		total++

		time.Sleep(3)
	}
}

func main() {
	fmt.Println("Startig Broker")

	context, err := gozmq.NewContext()
	if err != nil {
		panic(err)
	}
	defer context.Close()

	broker, err := context.NewSocket(gozmq.ROUTER)
	if err != nil {
		panic(err)
	}
	defer broker.Close()

	broker.Bind("tcp://*:9999")

	WORKER := 5

	for i := 0; i < WORKER; i++ {
		go worker_task()
	}

	endTime := time.Now().Unix() + 10
	fired := 0

	for {
		parts, err := broker.RecvMultipart(0)
		if err != nil {
			fmt.Println(err)
		}

		identify := parts[0]
		now := time.Now().Unix()
		if now < endTime {
			broker.SendMultipart([][]byte{identity, []byte(""), []byte("WORK")}, 0)
		} else {
			broker.SendMultipart([][]byte{identity, []byte(""), []byte("FIRED")}, 0)
			fired++
			if fired == WORKER {
				break
			}
		}
	}
}
