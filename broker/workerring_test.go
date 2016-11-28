package majordomogo

import (
	"fmt"
	"testing"
)

func TestWorkerRingNew(t *testing.T) {
	workerring := NewWorkerRing()
	if workerring == nil {
		t.Error("workerring is nil")
	}
}

func TestWorkerRingAdd(t *testing.T) {
	workerring := NewWorkerRing()
	newworker := NewWorker("newworker", "foobar")

	workerring.Add(newworker)
	if workerring.current == nil {
		t.Error("workerring add failed")
	}
}

func TestWorkerRingContainsZero(t *testing.T) {
	workerring := NewWorkerRing()

	if workerring.Contains("newworker") {
		t.Error("worker should not exist")
	}
}

func TestWorkerRingContainsOne(t *testing.T) {
	workerring := NewWorkerRing()
	newworker := NewWorker("newworker", "foobar")
	workerring.Add(newworker)

	if !workerring.Contains("newworker") {
		t.Error("workerring doesnt contain newworker")
	}
}

func TestWorkerRingContainsTwo(t *testing.T) {
	workerring := NewWorkerRing()
	newworker1 := NewWorker("newworker1", "foobar")
	newworker2 := NewWorker("newworker2", "foobar")
	workerring.Add(newworker1)
	workerring.Add(newworker2)

	if !workerring.Contains("newworker1") {
		t.Error("workerring doesnt contain newworker1")
	}

	if !workerring.Contains("newworker2") {
		t.Error("workerring doesnt contain newworker2")
	}
}

func TestWorkerRingDelete(t *testing.T) {
	workerring := NewWorkerRing()
	newworker1 := NewWorker("newworker1", "foobar")
	newworker2 := NewWorker("newworker2", "foobar")
	newworker3 := NewWorker("newworker3", "foobar")
	if workerring.Contains("newworker1") {
		t.Error("workerring somehow contains newworker1")
	}

	workerring.Add(newworker1)
	fmt.Printf("Ring: %s\n", workerring.ToString())
	if !workerring.Contains("newworker1") {
		t.Error("workerring doesnt contain newworker1")
	}

	workerring.Delete(newworker1)
	fmt.Printf("Ring: %s\n", workerring.ToString())
	if workerring.Contains("newworker1") {
		t.Error("workerring somehow contains newworker1")
	}

	workerring.Add(newworker1)
	fmt.Printf("Ring: %s\n", workerring.ToString())
	workerring.Add(newworker2)
	fmt.Printf("Ring: %s\n", workerring.ToString())
	workerring.Delete(newworker2)
	fmt.Printf("Ring: %s\n", workerring.ToString())
	if workerring.Contains("newworker2") {
		//fmt.Printf("workerring: %s\n", workerring.ToString())
		t.Error("workerring somehow contains newworker2")
	}

	workerring.Delete(newworker1)
	fmt.Printf("Ring: %s\n", workerring.ToString())
	if workerring.Contains("newworker1") {
		//fmt.Printf("workerring: %s\n", workerring.ToString())
		t.Error("workerring somehow contains newworker1")
	}

	workerring.Add(newworker1)
	fmt.Printf("Ring: %s\n", workerring.ToString())
	workerring.Add(newworker2)
	fmt.Printf("Ring: %s\n", workerring.ToString())
	workerring.Delete(newworker1)
	fmt.Printf("Ring: %s\n", workerring.ToString())
	if workerring.Contains("newworker1") {
		//fmt.Printf("workerring: %s\n", workerring.ToString())
		t.Error("workerring somehow contains newworker1")
	}

	workerring.Delete(newworker2)
	fmt.Printf("Ring: %s\n", workerring.ToString())
	if workerring.Contains("newworker2") {
		//fmt.Printf("workerring: %s\n", workerring.ToString())
		t.Error("workerring somehow contains newworker2")
	}

	workerring.Add(newworker1)
	fmt.Printf("Ring: %s\n", workerring.ToString())
	workerring.Add(newworker2)
	fmt.Printf("Ring: %s\n", workerring.ToString())
	workerring.Add(newworker3)
	fmt.Printf("Ring: %s\n", workerring.ToString())
	workerring.Delete(newworker2)
	fmt.Printf("Ring: %s\n", workerring.ToString())
	if workerring.Contains("newworker2") {
		t.Error("workerring somehow contains newworker2")
	}

	workerring.Delete(newworker3)
	fmt.Printf("Ring: %s\n", workerring.ToString())
	if workerring.Contains("newworker3") {
		//fmt.Printf("workerring: %s\n", workerring.ToString())
		t.Error("workerring somehow contains newworker3")
	}

	workerring.Delete(newworker1)
	fmt.Printf("Ring: %s\n", workerring.ToString())
	if workerring.Contains("newworker1") {
		//fmt.Printf("workerring: %s\n", workerring.ToString())
		t.Error("workerring somehow contains newworker1")
	}
}
