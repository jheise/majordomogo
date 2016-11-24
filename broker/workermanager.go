package main

type WorkerManager interface {
	Add(worker *Worker)
	Contains(worker string) bool
	Delete(worker *Worker) error
	GetFirst() (*Worker, error)
	GetSize() int
}
