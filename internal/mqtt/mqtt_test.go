package mqtt

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type capture struct {
	mu       sync.Mutex
	got      []string
	conns    atomic.Int64
	failNext atomic.Bool
}

func (c *capture) OnConnect(string)    { c.conns.Add(1) }
func (c *capture) OnDisconnect(string) { c.conns.Add(-1) }
func (c *capture) OnPublish(_ context.Context, id, topic string, p []byte, qos byte) error {
	if c.failNext.CompareAndSwap(true, false) {
		return errors.New("busy")
	}
	c.mu.Lock()
	c.got = append(c.got, fmt.Sprintf("%s|%s|%s", id, topic, p))
	c.mu.Unlock()
	return nil
}
func (c *capture) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.got)
}

func start(t *testing.T, cfg Config, h Handler) (*Server, string) {
	t.Helper()
	cfg.Addr = "127.0.0.1:0"
	srv := NewServer(cfg, h)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go srv.Serve(ln)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		srv.Shutdown(ctx)
	})
	return srv, ln.Addr().String()
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	for i := 0; i < 400; i++ {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timeout: %s", what)
}

func TestPublishQoS0And1(t *testing.T) {
	h := &capture{}
	_, addr := start(t, Config{}, h)
	c, err := Dial(addr, ClientOptions{ClientID: "dev1"})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	if err := c.Publish("devices/dev1/telemetry", []byte(`{"m":"x"}`), 1); err != nil {
		t.Fatal(err)
	}
	if err := c.Publish("devices/dev1/heartbeat", []byte("hb"), 0); err != nil {
		t.Fatal(err)
	}
	eventually(t, "both delivered", func() bool { return h.count() == 2 })
	if h.got[0] != `dev1|devices/dev1/telemetry|{"m":"x"}` {
		t.Fatalf("got %q", h.got[0])
	}
}

func TestQoS1NotAckedWhenHandlerFails(t *testing.T) {
	h := &capture{}
	h.failNext.Store(true)
	_, addr := start(t, Config{}, h)
	c, _ := Dial(addr, ClientOptions{ClientID: "d", Timeout: 150 * time.Millisecond})
	defer c.Close()

	if err := c.Publish("a/b", []byte("1"), 1); err == nil {
		t.Fatal("expected ack timeout")
	}
	c2, _ := Dial(addr, ClientOptions{ClientID: "d2"})
	defer c2.Close()
	if err := c2.Publish("a/b", []byte("2"), 1); err != nil {
		t.Fatal(err)
	}
}

func TestAuth(t *testing.T) {
	auth := func(id, user string, pass []byte) bool { return string(pass) == "s3cret" }
	_, addr := start(t, Config{Auth: auth}, &capture{})
	if _, err := Dial(addr, ClientOptions{ClientID: "x", Username: "u", Password: "wrong"}); err == nil {
		t.Fatal("bad password accepted")
	}
	c, err := Dial(addr, ClientOptions{ClientID: "x", Username: "u", Password: "s3cret"})
	if err != nil {
		t.Fatal(err)
	}
	c.Close()
}

func TestMaxConns(t *testing.T) {
	srv, addr := start(t, Config{MaxConns: 2}, &capture{})
	a, _ := Dial(addr, ClientOptions{ClientID: "a"})
	b, _ := Dial(addr, ClientOptions{ClientID: "b"})
	defer a.Close()
	defer b.Close()
	if _, err := Dial(addr, ClientOptions{ClientID: "c", Timeout: 500 * time.Millisecond}); err == nil {
		t.Fatal("third connection should be refused")
	}
	if srv.Stats().Rejected == 0 {
		t.Fatal("rejection not counted")
	}
}

func TestSessionTakeover(t *testing.T) {
	srv, addr := start(t, Config{}, &capture{})
	first, _ := Dial(addr, ClientOptions{ClientID: "same"})
	defer first.Close()
	second, _ := Dial(addr, ClientOptions{ClientID: "same"})
	defer second.Close()

	eventually(t, "old kicked", func() bool { return srv.Stats().Takeovers == 1 })
	if err := second.Publish("t/x", []byte("ok"), 1); err != nil {
		t.Fatal(err)
	}
}

func TestDownlink(t *testing.T) {
	srv, addr := start(t, Config{}, &capture{})
	msgs := make(chan string, 4)
	c, _ := Dial(addr, ClientOptions{ClientID: "dev9", OnMessage: func(topic string, p []byte) { msgs <- topic + "=" + string(p) }})
	defer c.Close()

	if err := srv.Publish("dev9", "devices/dev9/tasks", []byte("x")); !errors.Is(err, ErrNotSubscribed) {
		t.Fatalf("want ErrNotSubscribed, got %v", err)
	}
	if err := c.Subscribe("devices/dev9/tasks"); err != nil {
		t.Fatal(err)
	}
	if err := srv.Publish("dev9", "devices/dev9/tasks", []byte("reboot")); err != nil {
		t.Fatal(err)
	}
	if err := srv.Publish("nobody", "devices/nobody/tasks", nil); !errors.Is(err, ErrNotConnected) {
		t.Fatalf("got %v", err)
	}
	c.Poll(500 * time.Millisecond)
	select {
	case m := <-msgs:
		if m != "devices/dev9/tasks=reboot" {
			t.Fatalf("got %q", m)
		}
	default:
		t.Fatal("no downlink delivered")
	}
}

func TestIdleTimeout(t *testing.T) {
	h := &capture{}
	_, addr := start(t, Config{}, h)
	conn, _ := net.Dial("tcp", addr)
	defer conn.Close()
	conn.Write(buildConnect(connectPkt{clientID: "idle", keepAlive: 1, clean: true}))
	eventually(t, "connected", func() bool { return h.conns.Load() == 1 })
	// keepalive 1s => dropped after ~1.5s of silence
	eventually(t, "dropped", func() bool { return h.conns.Load() == 0 })
}

func TestGarbageDoesNotKillServer(t *testing.T) {
	srv, addr := start(t, Config{ConnectTimeout: 200 * time.Millisecond}, &capture{})
	for _, junk := range [][]byte{
		{0xff, 0xff, 0xff, 0xff, 0xff, 0x01},
		{0x10, 0x02, 0x00},
		{0x30, 0x00},
		bytes.Repeat([]byte{0x7f}, 100),
	} {
		c, _ := net.Dial("tcp", addr)
		c.Write(junk)
		c.Close()
	}
	eventually(t, "junk rejected", func() bool { return srv.Stats().Rejected >= 4 })
	c, err := Dial(addr, ClientOptions{ClientID: "still-alive"})
	if err != nil {
		t.Fatal(err)
	}
	c.Close()
}

func TestQoS2Rejected(t *testing.T) {
	h := &capture{}
	_, addr := start(t, Config{}, h)
	conn, _ := net.Dial("tcp", addr)
	defer conn.Close()
	conn.Write(buildConnect(connectPkt{clientID: "q2", keepAlive: 30, clean: true}))
	br := bufio.NewReader(conn)
	br.Discard(4)
	conn.Write(buildPublish("a/b", []byte("x"), 2, 1))
	conn.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := br.ReadByte(); err == nil {
		t.Fatal("server should have closed the connection")
	}
}

func TestTopicMatch(t *testing.T) {
	cases := []struct {
		f, t string
		want bool
	}{
		{"a/b", "a/b", true},
		{"a/b", "a/c", false},
		{"a/+/c", "a/x/c", true},
		{"a/+/c", "a/x/y/c", false},
		{"a/#", "a/b/c", true},
		{"a/#", "a", true},
		{"#", "x/y", true},
		{"+", "x", true},
		{"+", "x/y", false},
		{"a/b", "a/b/c", false},
		{"a/b/c", "a/b", false},
	}
	for _, c := range cases {
		if got := topicMatch(c.f, c.t); got != c.want {
			t.Errorf("topicMatch(%q,%q)=%v", c.f, c.t, got)
		}
	}
}

func TestRemainingLengthRoundTrip(t *testing.T) {
	for _, n := range []int{0, 1, 127, 128, 16383, 16384, 100000} {
		var buf []byte
		raw := append(append([]byte{0x30}, appendRemaining(nil, n)...), make([]byte, n)...)
		_, body, err := readPacket(bufio.NewReader(bytes.NewReader(raw)), 1<<20, &buf)
		if err != nil || len(body) != n {
			t.Fatalf("n=%d len=%d err=%v", n, len(body), err)
		}
	}
}

func FuzzParsers(f *testing.F) {
	f.Add(buildConnect(connectPkt{clientID: "a", username: "u", password: []byte("p"), keepAlive: 5}))
	f.Add(buildPublish("a/b", []byte("hi"), 1, 7))
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) < 1 {
			return
		}
		parseConnect(data)
		parsePublish(data[0], data[1:])
		parseSubscribe(data)
		parseUnsubscribe(data)
		var buf []byte
		readPacket(bufio.NewReader(bytes.NewReader(data)), 1<<16, &buf)
	})
}
