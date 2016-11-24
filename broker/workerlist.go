package main

import (
	// standard
	"errors"
	"fmt"
	"time"
)

type WorkerNode struct {
	worker *Worker
	next   *WorkerNode
}

type WorkerList struct {
	head *WorkerNode
	tail *WorkerNode
	size int
}

func (self *WorkerList) Add(worker *Worker) {
	newNode := new(WorkerNode)
	newNode.worker = worker

	// if tail exists add next
	if self.tail != nil {
		self.tail.next = newNode
	}

	// if head does not exist add it
	if self.head == nil {
		self.head = newNode
	}

	// set tail to new node
	self.tail = newNode
	self.size++
}

func (self *WorkerList) Contains(worker string) bool {

	for current := self.head; current != nil; current = current.next {
		if current.worker.ident == worker {
			return true
		}
	}

	return false
}

func (self *WorkerList) Delete(worker *Worker) error {
	fmt.Printf("Delete called for: %s\n", worker.ident)
	// if worker is head
	if self.head != nil && self.head.worker == worker {
		self.head = self.head.next
		self.size--
		return nil
	}

	var current *WorkerNode
	var prev *WorkerNode

	for current = self.head; current != nil; prev, current = current, current.next {
		if current.worker == worker {
			if prev != nil {
				prev.next = current.next
			} else {
				self.head = current.next
			}

			self.size--
			return nil
		}

		//prev = current
	}

	return errors.New("Worker not present")
}

func (self *WorkerList) Expire(now time.Time) error {
	current := self.head
	for current != nil {
		if current.worker.heartbeat.After(now) {
			break
		}
		old := current
		current = current.next
		self.Delete(old.worker)
	}

	return nil
}

func (self *WorkerList) GetFirst() (*Worker, error) {
	fmt.Printf("Getting first from list of size: %d\n", self.size)
	if self.head == nil {
		fmt.Printf("Unable to locate worker\n")
		if self.size > 0 {
			panic("self.head is nil, size is greater than 0")
		}
		return nil, errors.New("No head")
	}

	worker := self.head

	// if there is more than one worker for a service cycle
	if self.size > 1 {
		self.head = self.head.next
		self.tail.next = worker
		self.tail = worker
		self.tail.next = nil
	}

	return worker.worker, nil
}

func (self *WorkerList) GetSize() int {
	return self.size
}

func NewWorkerList() *WorkerList {
	newlist := new(WorkerList)
	return newlist
}
