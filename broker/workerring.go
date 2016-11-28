package majordomogo

import (
	"errors"
	"fmt"
	"strings"
)

type Node struct {
	worker *Worker
	next   *Node
	prev   *Node
}

type WorkerRing struct {
	size    int
	current *Node
}

func (self *Node) SetPrev(prev *Node) {
	self.prev = prev
}

func (self *Node) GetPrev() *Node {
	return self.prev
}

func (self *Node) SetNext(next *Node) {
	self.next = next
}

func (self *Node) GetNext() *Node {
	return self.next
}

func (self *Node) GetIdent() string {
	return self.worker.ident
}

func (self *Node) ToString() string {
	/*
	   var nextIdent string
	   var prevIdent string
	   next := self.GetNext()
	*/
	return fmt.Sprintf("Node: %s prev: %s next: %s", self.GetIdent(), self.GetPrev().GetIdent(), self.GetNext().GetIdent())
}

func NewNode(worker *Worker) *Node {
	node := new(Node)
	node.worker = worker

	return node
}

func (self *WorkerRing) Add(worker *Worker) {
	node := NewNode(worker)
	if self.current == nil {
		node.SetNext(node)
		node.SetPrev(node)
		self.current = node
	} else {
		self.current.GetNext().SetPrev(node)
		node.SetNext(self.current.GetNext())
		self.current.SetNext(node)
		node.SetPrev(self.current)
	}
	self.size++
}

func (self *WorkerRing) Delete(worker *Worker) error {
	// case: no nodes
	if self.current == nil {
		return errors.New("Empty List")
	}

	// case single node
	if self.current.GetNext().GetIdent() == self.current.GetIdent() {
		if self.current.GetIdent() == worker.ident {
			self.current = nil
			self.size = 0
			return nil
		}
		return errors.New("Worker not present")
	}

	// case two or more nodes
	for current := self.current; ; current = current.GetNext() {
		if current.GetIdent() == worker.ident {
			current.GetPrev().SetNext(current.GetNext())
			current.GetNext().SetPrev(current.GetPrev())
			self.size--
			if self.current == current {
				self.current = current.GetNext()
			}
			return nil
		}
		// if next node in the list is current, break
		if current.GetNext() == self.current {
			break
		}
	}

	return errors.New("Worker not present")
}

func (self *WorkerRing) Contains(ident string) bool {
	// case: no nodes
	if self.current == nil {
		return false
	}

	// case: one or greater number of nodes
	for current := self.current; ; current = current.GetNext() {
		if current.GetIdent() == ident {
			return true
		}
		if current.GetNext() == self.current {
			break
		}
	}

	return false
}

func (self *WorkerRing) GetFirst() (*Worker, error) {
	if self.current == nil {
		return nil, errors.New("Empty List")
	}

	value := self.current.worker
	self.current = self.current.GetNext()
	return value, nil
}

func (self *WorkerRing) ToString() string {
	if self.current == nil {
		return ""
	}
	var idents []string
	for current := self.current; ; current = current.GetNext() {
		idents = append(idents, current.GetIdent())

		if current.GetNext() == self.current {
			break
		}
	}

	return strings.Join(idents, " -> ")
}

func (self *WorkerRing) GetSize() int {
	return self.size
}

func NewWorkerRing() *WorkerRing {
	ring := new(WorkerRing)
	ring.size = 0

	return ring
}
