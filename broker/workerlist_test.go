package main

import (
	"testing"
	"time"
)

func TestWorkerListNew(t *testing.T) {
	workerlist := NewWorkerList()
	if workerlist == nil {
		t.Error("Workerlist is nil")
	}
}

func TestWorkerListAdd(t *testing.T) {
	workerlist := NewWorkerList()

	testworker := NewWorker("test", "test")
	workerlist.Add(testworker)
	notfound := true

	for current := workerlist.head; current != nil; current = current.next {
		if current.worker == testworker {
			notfound = false
			break
		}
	}

	if notfound {
		t.Error("Worker not added")
	}
}

func TestWorkerListContains(t *testing.T) {
	workerlist := NewWorkerList()

	testworker := NewWorker("test", "test")
	workerlist.Add(testworker)

	if !workerlist.Contains(testworker.ident) {
		t.Error("Worker not contained in list")
	}
}

func TestWorkerListDelete(t *testing.T) {
	workerlist := NewWorkerList()

	testworker := NewWorker("test", "test")
	workerlist.Add(testworker)

	workerlist.Delete(testworker)
	if workerlist.Contains(testworker.ident) {
		t.Error("Worker not deleted")
	}
}

func TestWorkerListExpire(t *testing.T) {
	workerlist := NewWorkerList()

	testworker := NewWorker("test", "test")
	workerlist.Add(testworker)

	workerlist.Expire(time.Now().Add(time.Millisecond * 30000))
	if workerlist.Contains(testworker.ident) {
		t.Error("Worker not expired")
	}
}

func TestWorkerListGetFirst(t *testing.T) {
	workerlist := NewWorkerList()

	testworker := NewWorker("test", "test")
	workerlist.Add(testworker)

	first, err := workerlist.GetFirst()
	if err != nil {
		t.Error(err)
	}

	if first != testworker {
		t.Error("Worker returned not worker expected")
	}
}
