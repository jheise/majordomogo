package main

import (
	"errors"
	"time"
)

type WorkerPool struct {
	workers map[string]*Worker
}

func NewWorkerPool() *WorkerPool {
	pool := new(WorkerPool)
	pool.workers = make(map[string]*Worker)

	return pool
}

func (self *WorkerPool) RegisterWorker(worker string, service string) *Worker {
	// check if worker already exists
	if val, ok := self.workers[worker]; ok {
		return val
	}

	newWorker := NewWorker(worker, service)
	self.workers[worker] = newWorker

	return newWorker
}

func (self *WorkerPool) RemoveWorker(worker string) {
	delete(self.workers, worker)
}

func (self *WorkerPool) HeartbeatWorker(ident string) {
	if worker, ok := self.workers[ident]; ok {
		worker.Heartbeat()
	}
}

func (self *WorkerPool) ExpireWorkers(current time.Time) []*Worker {
	removed := []*Worker{}
	for _, worker := range self.workers {
		if worker.heartbeat.Before(current) {

			removed = append(removed, worker)
			self.RemoveWorker(worker.ident)
		}
	}

	return removed
}

func (self *WorkerPool) GetWorkerService(worker string) (string, error) {
	if val, ok := self.workers[worker]; ok {
		return val.service, nil
	}

	return "", errors.New("Worker not registered")
}

func (self *WorkerPool) GetWorkers() []*Worker {
	workers := []*Worker{}
	for _, worker := range self.workers {
		workers = append(workers, worker)
	}

	return workers
}
