package mqtt

import (
	"bufio"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"
)

type ClientOptions struct {
	ClientID  string
	Username  string
	Password  string
	KeepAlive time.Duration
	Timeout   time.Duration // dial, connack and puback wait
	TLS       *tls.Config   // nil = plaintext
	// called for downlink publishes that show up while we are waiting on something else
	OnMessage func(topic string, payload []byte)
}

// Client is a minimal synchronous client, good enough for the simulator and
// tests. One caller at a time; calls are serialized internally.
type Client struct {
	opts ClientOptions
	conn net.Conn
	br   *bufio.Reader
	buf  []byte

	mu     sync.Mutex
	nextID uint16
}

func Dial(addr string, o ClientOptions) (*Client, error) {
	if o.Timeout <= 0 {
		o.Timeout = 5 * time.Second
	}
	if o.KeepAlive <= 0 {
		o.KeepAlive = 30 * time.Second
	}
	d := &net.Dialer{Timeout: o.Timeout}
	var conn net.Conn
	var err error
	if o.TLS != nil {
		conn, err = tls.DialWithDialer(d, "tcp", addr, o.TLS)
	} else {
		conn, err = d.Dial("tcp", addr)
	}
	if err != nil {
		return nil, err
	}
	c := &Client{opts: o, conn: conn, br: bufio.NewReaderSize(conn, 512)}

	pkt := buildConnect(connectPkt{clientID: o.ClientID, username: o.Username, keepAlive: uint16(o.KeepAlive / time.Second), clean: true, password: pw(o.Password)})
	conn.SetDeadline(time.Now().Add(o.Timeout))
	if _, err := conn.Write(pkt); err != nil {
		conn.Close()
		return nil, err
	}
	hdr, body, err := readPacket(c.br, 1024, &c.buf)
	if err != nil {
		conn.Close()
		return nil, err
	}
	if hdr>>4 != typeConnack || len(body) != 2 {
		conn.Close()
		return nil, ErrMalformed
	}
	if body[1] != connAccepted {
		conn.Close()
		return nil, fmt.Errorf("mqtt: connection refused (code %d)", body[1])
	}
	return c, nil
}

func pw(s string) []byte {
	if s == "" {
		return nil
	}
	return []byte(s)
}

func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.conn.Write([]byte{typeDisconnect << 4, 0})
	return c.conn.Close()
}

// Publish with qos 0 returns after the write; qos 1 waits for the PUBACK.
func (c *Client) Publish(topic string, payload []byte, qos byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	var id uint16
	if qos > 0 {
		c.nextID++
		if c.nextID == 0 {
			c.nextID = 1
		}
		id = c.nextID
	}
	c.conn.SetWriteDeadline(time.Now().Add(c.opts.Timeout))
	if _, err := c.conn.Write(buildPublish(topic, payload, qos, id)); err != nil {
		return err
	}
	if qos == 0 {
		return nil
	}
	return c.await(typePuback, id)
}

func (c *Client) Subscribe(filter string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.nextID++
	id := c.nextID
	body := []byte{byte(id >> 8), byte(id)}
	body = appendString(body, filter)
	body = append(body, 0)
	pkt := appendRemaining([]byte{typeSubscribe<<4 | 2}, len(body))
	c.conn.SetWriteDeadline(time.Now().Add(c.opts.Timeout))
	if _, err := c.conn.Write(append(pkt, body...)); err != nil {
		return err
	}
	return c.await(typeSuback, id)
}

func (c *Client) Ping() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.conn.SetWriteDeadline(time.Now().Add(c.opts.Timeout))
	_, err := c.conn.Write([]byte{typePingreq << 4, 0})
	return err
}

// Poll reads whatever is pending for up to d, delivering downlink publishes.
func (c *Client) Poll(d time.Duration) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.conn.SetReadDeadline(time.Now().Add(d))
	for {
		hdr, body, err := readPacket(c.br, 64<<10, &c.buf)
		if err != nil {
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				return nil
			}
			return err
		}
		c.dispatch(hdr, body)
	}
}

func (c *Client) await(want byte, id uint16) error {
	c.conn.SetReadDeadline(time.Now().Add(c.opts.Timeout))
	for {
		hdr, body, err := readPacket(c.br, 64<<10, &c.buf)
		if err != nil {
			return err
		}
		if hdr>>4 == want && len(body) >= 2 && uint16(body[0])<<8|uint16(body[1]) == id {
			return nil
		}
		c.dispatch(hdr, body)
	}
}

func (c *Client) dispatch(hdr byte, body []byte) {
	if hdr>>4 != typePublish || c.opts.OnMessage == nil {
		return // pingresp and friends
	}
	if p, err := parsePublish(hdr, body); err == nil {
		c.opts.OnMessage(p.topic, append([]byte(nil), p.payload...))
	}
}
