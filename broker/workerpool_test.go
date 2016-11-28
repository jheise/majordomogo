package majordomogo

import (
	"testing"
	"time"
)

func TestNewWorkerPool(t *testing.T) {
	pool := NewWorkerPool()
	if pool == nil {
		t.Error("Pool not created")
	}

	if pool.workers == nil {
		t.Error("pool.workers not created")
	}
}

func TestRegisterWorker(t *testing.T) {
	pool := NewWorkerPool()

	worker := pool.RegisterWorker(workername, service)
	if worker == nil {
		t.Error("worker cannot be nil")
	}

	if worker.ident != workername {
		t.Error("worker.ident and workername do not match")
	}

	secondworker := pool.RegisterWorker(workername, service)
	if worker != secondworker {
		t.Error("new worker was created, should have returned original")
	}
}

func TestRemoveWorker(t *testing.T) {
	pool := NewWorkerPool()

	workername := "foobar"

	pool.RegisterWorker(workername, service)

	pool.RemoveWorker(workername)

	if _, ok := pool.workers[workername]; ok {
		t.Error("worker not removed from pool")
	}
}

func TestExpireWorkers(t *testing.T) {
	pool := NewWorkerPool()

	pool.RegisterWorker(workername, service)
	pool.ExpireWorkers(time.Now().Add(30000 * time.Millisecond))

	if _, ok := pool.workers[workername]; ok {
		t.Error("worker not expired from pool")
	}

}
