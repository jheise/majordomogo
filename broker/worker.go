package main

import (
	// standard
	"time"
)

type Worker struct {
	ident     string
	service   string
	heartbeat time.Time
}

func (self *Worker) Heartbeat() {
	self.heartbeat = time.Now().Add(HEARTBEAT_INTERVAL * HEARTBEAT_LIVENESS)
}

func NewWorker(ident string, service string) *Worker {
	newWorker := new(Worker)
	newWorker.ident = ident
	newWorker.service = service
	newWorker.heartbeat = time.Now().Add(HEARTBEAT_INTERVAL * HEARTBEAT_LIVENESS)

	return newWorker
}
