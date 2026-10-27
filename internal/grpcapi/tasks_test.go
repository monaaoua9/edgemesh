package grpcapi

import (
	"context"
	"errors"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"edgemesh/internal/fleet"
	"edgemesh/internal/grpcapi/edgev1"
)

func TestTaskDownlink(t *testing.T) {
	r := newRig(t, "", nil)
	c := dial(t, r.addr, insecure.NewCredentials())
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := r.srv.Dispatch("d1", fleet.Task{ID: "x"}); !errors.Is(err, ErrNoTaskStream) {
		t.Fatalf("no stream yet, got %v", err)
	}

	st, err := c.Tasks(ctx, &edgev1.TaskRequest{DeviceId: "d1"})
	if err != nil {
		t.Fatal(err)
	}
	// the stream is only registered once the server handler runs
	for i := 0; ; i++ {
		err = r.srv.Dispatch("d1", fleet.Task{ID: "t1", Kind: "reboot", Payload: []byte(`{"a":1}`), Attempts: 2})
		if err == nil {
			break
		}
		if i > 100 {
			t.Fatal(err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	got, err := st.Recv()
	if err != nil {
		t.Fatal(err)
	}
	if got.Id != "t1" || got.Kind != "reboot" || string(got.Payload) != `{"a":1}` || got.Attempts != 2 {
		t.Fatalf("got %+v", got)
	}
}

func TestTaskStreamReplaced(t *testing.T) {
	r := newRig(t, "", nil)
	c := dial(t, r.addr, insecure.NewCredentials())
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	old, _ := c.Tasks(ctx, &edgev1.TaskRequest{DeviceId: "d1"})
	for r.srv.Dispatch("d1", fleet.Task{ID: "probe"}) != nil {
		time.Sleep(10 * time.Millisecond)
	}
	old.Recv() // drain the probe

	fresh, _ := c.Tasks(ctx, &edgev1.TaskRequest{DeviceId: "d1"})
	if _, err := old.Recv(); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("old stream should be replaced, got %v", err)
	}
	if err := r.srv.Dispatch("d1", fleet.Task{ID: "t2"}); err != nil {
		t.Fatal(err)
	}
	if got, err := fresh.Recv(); err != nil || got.Id != "t2" {
		t.Fatalf("got %+v %v", got, err)
	}
}

func TestTaskBadDevice(t *testing.T) {
	r := newRig(t, "", nil)
	c := dial(t, r.addr, insecure.NewCredentials())
	st, _ := c.Tasks(context.Background(), &edgev1.TaskRequest{DeviceId: "a/b"})
	if _, err := st.Recv(); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("got %v", err)
	}
}
