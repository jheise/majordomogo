package main

import (
	// standard
	"errors"
)

type ServiceContainer struct {
	Name    string
	Workers WorkerManager
}

func (self *ServiceContainer) DeleteWorker(worker *Worker) error {
	err := self.Workers.Delete(worker)
	if err != nil {
		return err
	}

	return nil
}

func (self *ServiceContainer) AddWorker(newWorker *Worker) {
	if !self.Workers.Contains(newWorker.ident) {
		self.Workers.Add(newWorker)
	}
}

func (self *ServiceContainer) GetWorker() (*Worker, error) {
	if self.Workers.GetSize() == 0 {
		return nil, errors.New("No workers available")
	}
	worker, err := self.Workers.GetFirst()
	if err != nil {
		return nil, err
	}

	return worker, nil
}

func NewServiceContainer(name string) *ServiceContainer {
	newContainer := new(ServiceContainer)
	newContainer.Workers = NewWorkerRing()
	newContainer.Name = name
	return newContainer
}
