package main

import (
	// standard
	// "errors"
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

type Broker struct {
	Services  map[string]*ServiceContainer
	workers   *WorkerPool
	context   *zmq.Context
	socket    *zmq.Socket
	poller    *zmq.Poller
	heartbeat time.Time
}

func (self *Broker) RegisterWorker(sender string, service string) {
	// create a new container if needed
	if _, ok := self.Services[service]; !ok {
		self.Services[service] = NewServiceContainer(service)
	}

	// register the worker in the worker pool
	worker := self.workers.RegisterWorker(sender, service)

	self.Services[service].AddWorker(worker)
}

func (self *Broker) LookupService(sender string) (string, error) {
	fmt.Println("Doing lookup for " + sender)
	service, err := self.workers.GetWorkerService(sender)
	return service, err
	//for service, container := range self.Services {
	//	if container.Workers.Contains(sender) {
	//		return service, nil
	//	}
	//}

	//return "", errors.New("Worker not registered to serivce")
}

func (self *Broker) SendClientRequest(client string, service string, msg [][]byte) {
	if _, ok := self.Services[service]; !ok {
		fmt.Printf("No worker registered for service: %s\n", service)
		return
	}

	targetWorker, err := self.Services[service].GetWorker()
	if err != nil {
		fmt.Printf("No worker found for Service: %s\n", service)
		return
	}

	worker := []byte(targetWorker.ident)
	frame0 := []byte("")
	frame1 := []byte(MDPW_WORKER)
	frame2 := []byte(MDPW_REQUEST)
	frame3 := []byte(client)
	frame4 := []byte("")
	frame5 := msg
	data := append([][]byte{worker, frame0, frame1, frame2, frame3, frame4}, frame5...)

	self.socket.SendMessage(data)
}

func (self *Broker) processClient(sender string, msg [][]byte) {
	service := string(msg[0])
	msg = msg[1:]
	fmt.Printf("Client: %s Service: %s\n", sender, service)
	self.SendClientRequest(sender, service, msg)
}

func (self *Broker) processWorker(sender string, msg [][]byte) {
	opcode, msg := msg[0], msg[1:]
	//fmt.Println(msg)

	switch string(opcode) {
	case MDPW_READY:
		service := string(msg[0])
		self.workerReady(sender, service)
	case MDPW_REPLY:
		client := string(msg[0])
		msg = msg[1:]
		self.workerReply(sender, client, msg)
	case MDPW_HEARTBEAT:
		self.ReceiveHeartbeat(sender)
	case MDPW_DISCONNECT:
		workerDisconnect(sender)
	}
}

func (self *Broker) workerReady(sender string, service string) {
	fmt.Printf("Readying %s for service %s\n", sender, service)
	self.RegisterWorker(sender, service)
}

func (self *Broker) sendClientResponse(client string, service string, msg [][]byte) {
	target := []byte(client)
	frame0 := []byte("")
	frame1 := []byte(MDPC_CLIENT)
	frame2 := []byte(service)
	frame3 := msg
	data := append([][]byte{target, frame0, frame1, frame2}, frame3...)

	self.socket.SendMessage(data)
}

func (self *Broker) workerReply(sender string, client string, msg [][]byte) {
	fmt.Printf("Reply %s, %s, %s", sender, client, msg)
	// look up service sender is from then send to client
	service, err := self.LookupService(sender)
	if err != nil {
		fmt.Println(err)
		return
	}
	self.sendClientResponse(client, service, msg)
}

func (self *Broker) SendHeartbeat(sender string) {
	fmt.Printf("Heartbeat %s\n", sender)
	target := []byte(sender)
	frame0 := []byte("")
	frame1 := []byte(MDPW_WORKER)
	frame2 := []byte(MDPW_HEARTBEAT)
	data := [][]byte{target, frame0, frame1, frame2}

	self.socket.SendMessage(data)
}

func (self *Broker) ReceiveHeartbeat(sender string) {
	//fmt.Printf("Received Heartbeat from %s\n", sender)
	self.workers.HeartbeatWorker(sender)
}

func workerDisconnect(sender string) {
	fmt.Printf("Disconnecting %s\n", sender)
}

func NewBroker(connect string) (*Broker, error) {
	fmt.Println("Starting broker on " + connect)
	context, err := zmq.NewContext()
	if err != nil {
		return nil, err
	}

	socket, err := context.NewSocket(zmq.ROUTER)
	if err != nil {
		return nil, err
	}
	socket.Bind(connect)
	nm := new(Broker)
	nm.Services = make(map[string]*ServiceContainer)
	nm.workers = NewWorkerPool()
	nm.context = context
	nm.socket = socket
	nm.poller = zmq.NewPoller()
	nm.poller.Add(nm.socket, zmq.POLLIN)
	nm.heartbeat = time.Now().Add(HEARTBEAT_INTERVAL)
	return nm, nil
}

func (self *Broker) Run() {
	for {

		sockets, _ := self.poller.Poll(HEARTBEAT_INTERVAL)

		if len(sockets) > 0 {
			s := sockets[0].Socket

			msg, err := s.RecvMessageBytes(0)
			if err != nil {
				fmt.Println(err)
			}

			sender := string(msg[0])
			header := msg[2]
			msg = msg[3:]

			if string(header) == MDPW_WORKER {
				self.processWorker(sender, msg)
			} else if string(header) == MDPC_CLIENT {
				self.processClient(sender, msg)
			} else {
				fmt.Println("invaild message")
				fmt.Println(string(header))
			}

		}

		// Process heartbeats
		if self.heartbeat.Before(time.Now()) {
			fmt.Printf("Sending Heartbeats %s\n", time.Now())

			self.heartbeat = time.Now().Add(HEARTBEAT_INTERVAL)

			// for each service remove old workers
			expired := self.workers.ExpireWorkers(self.heartbeat)
			for _, worker := range expired {
				fmt.Println("Expired: " + worker.ident)
				//self.Services[worker.service].DeleteWorker(worker)
				err := self.Services[worker.service].DeleteWorker(worker)
				if err != nil {
					panic(err)
				}
			}

			// for all remaining workers send heartbeat
			for _, worker := range self.workers.GetWorkers() {
				fmt.Println("Sending hearbeat to worker: " + worker.ident)
				self.SendHeartbeat(worker.ident)
			}
		}

	}

}

func (self *Broker) Close() {
	if self.socket != nil {
		self.socket.Close()
	}
	self.context.Term()
}
