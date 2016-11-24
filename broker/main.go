package main

func main() {
	broker, err := NewBroker("tcp://127.0.0.1:9999")
	if err != nil {
		panic(err)
	}
	defer broker.Close()

	broker.Run()
}
