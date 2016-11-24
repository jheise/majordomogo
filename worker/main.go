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
	HEARTBEAT_LIVENESS = 3
	MDPW_WORKER        = "MDPW01"
	MDPC_CLIENT        = "MDPC01"
	MDPW_READY         = "\001"
	MDPW_REQUEST       = "\002"
	MDPW_REPLY         = "\003"
	MDPW_HEARTBEAT     = "\004"
	MDPW_DISCONNECT    = "\005"
)

type MDWorker struct {
	context  *zmq.Context
	socket   *zmq.Socket
	poller   *zmq.Poller
	broker   string
	service  string
	ident    string
	liveness int
}

func (self *MDWorker) sendReady() error {
	fmt.Println("sendReady called")
	frame0 := []byte("")
	frame1 := []byte(MDPW_WORKER)
	frame2 := []byte(MDPW_READY)
	frame3 := []byte(self.service)
	data := [][]byte{frame0, frame1, frame2, frame3}

	_, err := self.socket.SendMessage(data, 0)
	if err != nil {
		return err
	}

	return nil
}

func (self *MDWorker) sendReply(client []byte, msg []byte) {
	frame0 := []byte("")
	frame1 := []byte(MDPW_WORKER)
	frame2 := []byte(MDPW_REPLY)
	frame3 := client
	frame4 := []byte("")
	frame5 := msg
	data := [][]byte{frame0, frame1, frame2, frame3, frame4, frame5}

	self.socket.SendMessage(data, 0)
}

func (self *MDWorker) sendHeartbeat() {
	frame0 := []byte("")
	frame1 := []byte(MDPW_WORKER)
	frame2 := []byte(MDPW_HEARTBEAT)
	data := [][]byte{frame0, frame1, frame2}

	self.socket.SendMessage(data, 0)
}

func (self *MDWorker) sendDisconnect() {
	frame0 := []byte("")
	frame1 := []byte(MDPW_WORKER)
	frame2 := []byte(MDPW_DISCONNECT)
	data := [][]byte{frame0, frame1, frame2}

	self.socket.SendMessage(data, 0)
}

func (self *MDWorker) processDisconnect() {
	fmt.Println("DISCONNECT")
}

func (self *MDWorker) processHeartbeat() {
	fmt.Printf("Heartbeat %s\n", time.Now())
}

func (self *MDWorker) processRequest(client []byte, msg [][]byte) {
	fmt.Println("Responding to Client: " + string(client))
	self.sendReply(client, []byte("HELLO"))
}

func (self *MDWorker) processMessage(msg [][]byte) {
	opcode, msg := msg[0], msg[1:]

	switch string(opcode) {
	case MDPW_REQUEST:
		client := msg[0]
		msg = msg[2:]
		self.processRequest(client, msg)
	case MDPW_HEARTBEAT:
		self.processHeartbeat()
	case MDPW_DISCONNECT:
		self.processDisconnect()
	}

}

func NewWorker(broker string, service string) (*MDWorker, error) {
	worker := new(MDWorker)

	context, err := zmq.NewContext()
	if err != nil {
		return nil, err
	}
	worker.context = context

	socket, err := context.NewSocket(zmq.DEALER)
	if err != nil {
		return nil, err
	}
	worker.socket = socket

	worker.ident = fmt.Sprintf("worker%d", time.Now().Unix())
	worker.socket.SetIdentity(worker.ident)
	worker.socket.Connect(broker)

	worker.poller = zmq.NewPoller()
	worker.poller.Add(worker.socket, zmq.POLLIN)

	worker.liveness = HEARTBEAT_LIVENESS
	worker.service = service
	worker.broker = broker

	return worker, nil
}

func (self *MDWorker) reconnect() error {
	fmt.Println("Attempting reconnect")
	if self.socket != nil {
		self.socket.Close()
	}

	socket, err := self.context.NewSocket(zmq.DEALER)
	if err != nil {
		return err
	}
	self.socket = socket

	self.socket.SetIdentity(self.ident)
	err = self.socket.Connect(self.broker)
	if err != nil {
		panic(err)
	}

	self.poller = zmq.NewPoller()
	self.poller.Add(self.socket, zmq.POLLIN)

	self.liveness = HEARTBEAT_LIVENESS

	return nil
}

func (self *MDWorker) Work() {
	for {

		sockets, _ := self.poller.Poll(HEARTBEAT_INTERVAL)

		if len(sockets) > 0 {
			s := sockets[0].Socket
			msg, _ := s.RecvMessageBytes(0)
			self.liveness = HEARTBEAT_LIVENESS
			header := msg[1]
			msg = msg[2:]
			if string(header) == MDPW_WORKER {
				self.processMessage(msg)
			} else {
				fmt.Println("invalid message")
				fmt.Println(string(header))
			}
		} else if self.liveness--; self.liveness <= 0 {
			time.Sleep(HEARTBEAT_INTERVAL)
			err := self.reconnect()
			if err != nil {
				panic(err)
			}
			fmt.Println("Sending Ready to server")
			err = self.sendReady()
			if err != nil {
				panic(err)
			}
		}

		// do heartbeat
		self.sendHeartbeat()

	}
}

func main() {
	fmt.Println("Creating worker")
	worker, err := NewWorker("tcp://localhost:9999", "basic")
	if err != nil {
		panic(err)
	}
	fmt.Println("Starting worker: " + worker.ident)

	err = worker.sendReady()
	if err != nil {
		panic(err)
	}

	worker.Work()
}
