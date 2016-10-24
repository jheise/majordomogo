package main

import (
	// standard
	"fmt"
	"time"

	// external
	zmq "github.com/pebbe/zmq4"
)

const (
	HEARTBEAT_INTERVAL = 2500 * time.Millisecond
)

func main() {
	fmt.Println("Startig Broker")

	context, err := zmq.NewContext()
	if err != nil {
		panic(err)
	}
	defer context.Term()

	broker, err := context.NewSocket(zmq.ROUTER)
	if err != nil {
		panic(err)
	}
	defer broker.Close()

	broker.Bind("tcp://*:9999")

	poller := zmq.NewPoller()
	poller.Add(broker, zmq.POLLIN)

	for {

		sockets, _ := poller.Poll(HEARTBEAT_INTERVAL)

		if len(sockets) > 0 {
			s := sockets[0].Socket

			msg, err := s.RecvMessageBytes(0)
			if err != nil {
				fmt.Println(err)
			}

			sender := msg[0]
			header := msg[2]
			//msg = msg[3:]

			fmt.Println(string(sender))
			fmt.Println(string(header))
		}

	}
}
