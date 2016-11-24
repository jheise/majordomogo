package main

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
	ident   string
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
	socket.Connect("tcp://localhost:9999")

	client.context = context
	client.socket = socket
	client.ident = ident

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
	client.sendRequest([]byte("basic"), []byte("HELLO"))
	for x := 0; x < 3; x++ {
		// wait for response
		output, _ := client.socket.RecvMessageBytes(0)

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

	return nil, errors.New("No Message Received")
}

func main() {
	client, err := NewMDClient("tcp://localhost:9999")
	if err != nil {
		panic(err)
	}

	output, err := client.MakeReq("basic", "hello")
	if err != nil {
		panic(err)
	}
	for _, part := range output {
		fmt.Println(string(part))
	}

}
