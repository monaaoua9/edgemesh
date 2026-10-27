// Package mqtt is a deliberately small MQTT 3.1.1 implementation: enough of
// the spec (QoS 0/1, subscribe, keepalive) for devices to talk to the gateway
// without dragging in a full broker.
package mqtt

import (
	"bufio"
	"errors"
	"io"
	"strings"
)

const (
	typeConnect     = 1
	typeConnack     = 2
	typePublish     = 3
	typePuback      = 4
	typeSubscribe   = 8
	typeSuback      = 9
	typeUnsubscribe = 10
	typeUnsuback    = 11
	typePingreq     = 12
	typePingresp    = 13
	typeDisconnect  = 14
)

const (
	connAccepted     = 0
	connBadProtocol  = 1
	connBadClientID  = 2
	connUnavailable  = 3
	connBadCreds     = 4
	connNotAuthorize = 5
)

var (
	ErrMalformed   = errors.New("mqtt: malformed packet")
	ErrTooLarge    = errors.New("mqtt: packet too large")
	ErrUnsupported = errors.New("mqtt: unsupported feature")
)

type connectPkt struct {
	clientID  string
	username  string
	password  []byte
	keepAlive uint16
	clean     bool
}

type publishPkt struct {
	topic   string
	payload []byte
	id      uint16
	qos     byte
	dup     bool
	retain  bool
}

type subscribePkt struct {
	id      uint16
	filters []string
}

// body is only valid until the next call that reuses buf
func readPacket(r *bufio.Reader, maxSize int, buf *[]byte) (hdr byte, body []byte, err error) {
	hdr, err = r.ReadByte()
	if err != nil {
		return 0, nil, err
	}
	n, mult := 0, 1
	for i := 0; ; i++ {
		if i == 4 {
			return 0, nil, ErrMalformed
		}
		b, err := r.ReadByte()
		if err != nil {
			return 0, nil, unexpected(err)
		}
		n += int(b&0x7f) * mult
		if b&0x80 == 0 {
			break
		}
		mult *= 128
	}
	if n > maxSize {
		return 0, nil, ErrTooLarge
	}
	if cap(*buf) < n {
		*buf = make([]byte, n, max(n, 256))
	}
	body = (*buf)[:n]
	if _, err := io.ReadFull(r, body); err != nil {
		return 0, nil, unexpected(err)
	}
	return hdr, body, nil
}

func unexpected(err error) error {
	if err == io.EOF {
		return io.ErrUnexpectedEOF
	}
	return err
}

func appendRemaining(dst []byte, n int) []byte {
	for {
		b := byte(n % 128)
		n /= 128
		if n > 0 {
			b |= 0x80
		}
		dst = append(dst, b)
		if n == 0 {
			return dst
		}
	}
}

func readString(b []byte) (string, []byte, error) {
	if len(b) < 2 {
		return "", nil, ErrMalformed
	}
	n := int(b[0])<<8 | int(b[1])
	if len(b) < 2+n {
		return "", nil, ErrMalformed
	}
	return string(b[2 : 2+n]), b[2+n:], nil
}

func readBytes(b []byte) ([]byte, []byte, error) {
	if len(b) < 2 {
		return nil, nil, ErrMalformed
	}
	n := int(b[0])<<8 | int(b[1])
	if len(b) < 2+n {
		return nil, nil, ErrMalformed
	}
	return b[2 : 2+n], b[2+n:], nil
}

func appendString(dst []byte, s string) []byte {
	dst = append(dst, byte(len(s)>>8), byte(len(s)))
	return append(dst, s...)
}

func parseConnect(b []byte) (connectPkt, byte, error) {
	var c connectPkt
	proto, b, err := readString(b)
	if err != nil {
		return c, 0, err
	}
	if len(b) < 4 {
		return c, 0, ErrMalformed
	}
	level, flags := b[0], b[1]
	c.keepAlive = uint16(b[2])<<8 | uint16(b[3])
	b = b[4:]
	if proto != "MQTT" || level != 4 {
		return c, connBadProtocol, ErrUnsupported
	}
	if flags&1 != 0 { // reserved bit must be 0
		return c, 0, ErrMalformed
	}
	c.clean = flags&0x02 != 0

	if c.clientID, b, err = readString(b); err != nil {
		return c, 0, err
	}
	if flags&0x04 != 0 { // will: topic + message, we ignore it but have to skip it
		if _, b, err = readString(b); err != nil {
			return c, 0, err
		}
		if _, b, err = readBytes(b); err != nil {
			return c, 0, err
		}
	}
	if flags&0x80 != 0 {
		if c.username, b, err = readString(b); err != nil {
			return c, 0, err
		}
	}
	if flags&0x40 != 0 {
		var p []byte
		if p, _, err = readBytes(b); err != nil {
			return c, 0, err
		}
		c.password = append([]byte(nil), p...)
	}
	return c, connAccepted, nil
}

func parsePublish(hdr byte, b []byte) (publishPkt, error) {
	var p publishPkt
	p.dup = hdr&0x08 != 0
	p.qos = (hdr >> 1) & 3
	p.retain = hdr&1 != 0
	if p.qos == 3 {
		return p, ErrMalformed
	}
	var err error
	if p.topic, b, err = readString(b); err != nil {
		return p, err
	}
	if p.topic == "" || strings.ContainsAny(p.topic, "+#") {
		return p, ErrMalformed
	}
	if p.qos > 0 {
		if len(b) < 2 {
			return p, ErrMalformed
		}
		p.id = uint16(b[0])<<8 | uint16(b[1])
		b = b[2:]
	}
	p.payload = b
	return p, nil
}

func parseSubscribe(b []byte) (subscribePkt, error) {
	var s subscribePkt
	if len(b) < 2 {
		return s, ErrMalformed
	}
	s.id = uint16(b[0])<<8 | uint16(b[1])
	b = b[2:]
	if len(b) == 0 {
		return s, ErrMalformed
	}
	for len(b) > 0 {
		f, rest, err := readString(b)
		if err != nil || len(rest) < 1 || f == "" {
			return s, ErrMalformed
		}
		s.filters = append(s.filters, f)
		b = rest[1:] // requested qos, we always grant 0
	}
	return s, nil
}

func parseUnsubscribe(b []byte) (id uint16, filters []string, err error) {
	if len(b) < 2 {
		return 0, nil, ErrMalformed
	}
	id = uint16(b[0])<<8 | uint16(b[1])
	b = b[2:]
	for len(b) > 0 {
		var f string
		if f, b, err = readString(b); err != nil {
			return 0, nil, err
		}
		filters = append(filters, f)
	}
	return id, filters, nil
}

func buildPublish(topic string, payload []byte, qos byte, id uint16) []byte {
	n := 2 + len(topic) + len(payload)
	if qos > 0 {
		n += 2
	}
	out := make([]byte, 0, n+5)
	out = append(out, typePublish<<4|qos<<1)
	out = appendRemaining(out, n)
	out = appendString(out, topic)
	if qos > 0 {
		out = append(out, byte(id>>8), byte(id))
	}
	return append(out, payload...)
}

func buildConnect(c connectPkt) []byte {
	var flags byte
	body := appendString(nil, "MQTT")
	body = append(body, 4)
	if c.clean {
		flags |= 0x02
	}
	if c.username != "" {
		flags |= 0x80
	}
	if c.password != nil {
		flags |= 0x40
	}
	body = append(body, flags, byte(c.keepAlive>>8), byte(c.keepAlive))
	body = appendString(body, c.clientID)
	if c.username != "" {
		body = appendString(body, c.username)
	}
	if c.password != nil {
		body = append(body, byte(len(c.password)>>8), byte(len(c.password)))
		body = append(body, c.password...)
	}
	out := []byte{typeConnect << 4}
	out = appendRemaining(out, len(body))
	return append(out, body...)
}

// +  matches exactly one level, # matches the rest (must be last)
func topicMatch(filter, topic string) bool {
	for {
		fi := strings.IndexByte(filter, '/')
		ti := strings.IndexByte(topic, '/')
		var fl, tl string
		if fi < 0 {
			fl = filter
		} else {
			fl = filter[:fi]
		}
		if ti < 0 {
			tl = topic
		} else {
			tl = topic[:ti]
		}

		if fl == "#" {
			return true
		}
		if fl != "+" && fl != tl {
			return false
		}
		if ti < 0 && fi >= 0 && filter[fi+1:] == "#" {
			return true // "a/#" also matches "a"
		}
		if fi < 0 || ti < 0 {
			return fi < 0 && ti < 0
		}
		filter, topic = filter[fi+1:], topic[ti+1:]
	}
}
