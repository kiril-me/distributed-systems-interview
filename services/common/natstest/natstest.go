// Package natstest gives tests a real JetStream server in process, and a TCP
// proxy in front of it that can be frozen to stand for a network partition.
package natstest

import (
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

// Start runs a NATS server with JetStream and returns its client URL.
func Start(t *testing.T) string {
	t.Helper()
	srv, err := server.NewServer(&server.Options{Port: -1, JetStream: true, StoreDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	go srv.Start()
	if !srv.ReadyForConnections(5 * time.Second) {
		t.Fatal("nats not ready")
	}
	t.Cleanup(srv.Shutdown)
	return srv.ClientURL()
}

// Connect opens a client connection that is closed when the test ends.
func Connect(t *testing.T, url string) *nats.Conn {
	t.Helper()
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	return nc
}

// Proxy forwards TCP to a NATS server. Freeze stops forwarding in both
// directions without closing anything, like a partition: both ends keep
// writing into their socket buffers and see no error. Thaw delivers what
// was held back, in order.
type Proxy struct {
	URL string

	mu     sync.Mutex
	frozen bool
	thawed *sync.Cond
	conns  []net.Conn
}

// NewProxy listens on a free port and forwards every connection to target.
func NewProxy(t *testing.T, target string) *Proxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &Proxy{URL: "nats://" + ln.Addr().String()}
	p.thawed = sync.NewCond(&p.mu)
	t.Cleanup(func() {
		ln.Close()
		p.Thaw()
		p.mu.Lock()
		defer p.mu.Unlock()
		for _, c := range p.conns {
			c.Close()
		}
	})
	addr := target[len("nats://"):]

	go func() {
		for {
			client, err := ln.Accept()
			if err != nil {
				return
			}
			upstream, err := net.Dial("tcp", addr)
			if err != nil {
				client.Close()
				continue
			}
			p.mu.Lock()
			p.conns = append(p.conns, client, upstream)
			p.mu.Unlock()
			go p.pipe(upstream, client)
			go p.pipe(client, upstream)
		}
	}()
	return p
}

func (p *Proxy) Freeze() {
	p.mu.Lock()
	p.frozen = true
	p.mu.Unlock()
}

func (p *Proxy) Thaw() {
	p.mu.Lock()
	p.frozen = false
	p.mu.Unlock()
	p.thawed.Broadcast()
}

// pipe copies src to dst, holding each chunk while the proxy is frozen.
func (p *Proxy) pipe(dst io.Writer, src io.Reader) {
	buf := make([]byte, 32*1024)
	for {
		n, err := src.Read(buf)
		if err != nil {
			return
		}
		p.mu.Lock()
		for p.frozen {
			p.thawed.Wait()
		}
		p.mu.Unlock()
		if _, err := dst.Write(buf[:n]); err != nil {
			return
		}
	}
}
