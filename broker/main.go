package main

import (
	// standard
	"fmt"

	// external
	"github.com/alecthomas/gozmq"
)

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

	for {
		parts, err := broker.RecvMultipart(0)
		if err != nil {
			fmt.Println(err)
		}

		identity := parts[0]
		fmt.Println("Serving worker: " + string(identity))
		broker.SendMultipart([][]byte{identity, []byte(""), []byte("WORK")}, 0)
	}
}
