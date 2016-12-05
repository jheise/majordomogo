package majordomogo

import (
	// standard
	"errors"
	"fmt"
	"time"

	// external
	zmq "github.com/pebbe/zmq4"
)

const (
	HEARTBEAT_INTERVAL = 2500 * time.Millisecond
	MDPW_WORKER        = "MDPW01"
	MDPC_CLIENT        = "MDPC01"
	MDPW_READY         = "\001"
	MDPW_REQUEST       = "\002"
	MDPW_REPLY         = "\003"
	MDPW_HEARTBEAT     = "\004"
	MDPW_DISCONNECT    = "\005"
)

type MDClient struct {
	context *zmq.Context
	socket  *zmq.Socket
	poller  *zmq.Poller
	ident   string
	server  string
}

func NewMDClient(connect string) (*MDClient, error) {
	client := new(MDClient)

	context, err := zmq.NewContext()
	if err != nil {
		return nil, err
	}

	socket, err := context.NewSocket(zmq.DEALER)
	if err != nil {
		return nil, err
	}
	ident := fmt.Sprintf("client%d", time.Now().Unix())
	socket.SetIdentity(ident)
	socket.Connect(connect)

	poller := zmq.NewPoller()
	poller.Add(socket, zmq.POLLIN)

	client.context = context
	client.socket = socket
	client.ident = ident
	client.server = connect
	client.poller = poller

	return client, nil
}

func (client *MDClient) sendRequest(service []byte, msg []byte) {
	frame0 := []byte("")
	frame1 := []byte(MDPC_CLIENT)
	frame2 := service
	frame3 := msg
	data := [][]byte{frame0, frame1, frame2, frame3}

	client.socket.SendMessage(data, 0)
}

func (client *MDClient) MakeReq(service string, msg string) ([][]byte, error) {
	// send message
	client.sendRequest([]byte(service), []byte(msg))
	for x := 0; x < 3; x++ {
		sockets, _ := client.poller.Poll(HEARTBEAT_INTERVAL)
		// wait for response
		if len(sockets) > 0 {
			s := sockets[0].Socket
			output, err := s.RecvMessageBytes(0)
			if err != nil {
				return nil, err
			}

			header := output[1]
			output = output[2:]

			if string(header) != MDPC_CLIENT {
				return nil, errors.New("Invalid header")
			}

			outputService := string(output[0])
			if outputService == service {

				output = output[2:]
				return output, nil
			}
		}

	}

	return nil, errors.New("No Message Received")
}
