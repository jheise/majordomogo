package majordomogo

import (
	"testing"
)

var (
	workername = "foobar"
	service    = "foobar"
)

func TestNewWorker(t *testing.T) {
	worker := NewWorker(workername, service)
	if worker == nil {
		t.Error("Worker not created")
	}

	if worker.ident != workername {
		t.Error("Worker.ident not set")
	}

	if worker.service != service {
		t.Error("Worker.service not set")
	}

}
