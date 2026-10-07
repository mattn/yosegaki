package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/coder/websocket"
)

func usage() {
	fmt.Fprintf(os.Stderr, `usage:
  yosegaki serve [-addr :8080] [-real-ip-header CF-Connecting-IP] [-max-memory 96]
                                 run the server
  yosegaki connect URL           bridge stdin/stdout to the server (used by vim-yosegaki)
  yosegaki list URL              print public sessions as JSON
`)
	os.Exit(2)
}

// connect relays newline separated JSON between stdio and the WebSocket,
// because Vim channels cannot speak WebSocket or TLS.
func connect(url string) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c, _, err := websocket.Dial(ctx, url, nil)
	if err != nil {
		var opErr *net.OpError
		if errors.As(err, &opErr) {
			err = opErr.Err
		}
		return fmt.Errorf("cannot connect to %s: %v", url, err)
	}
	defer c.CloseNow()
	c.SetReadLimit(maxMessageSize)

	errc := make(chan error, 2)
	go func() {
		sc := bufio.NewScanner(os.Stdin)
		sc.Buffer(make([]byte, 64*1024), maxMessageSize)
		for sc.Scan() {
			if len(sc.Bytes()) == 0 {
				continue
			}
			if err := c.Write(ctx, websocket.MessageText, sc.Bytes()); err != nil {
				errc <- err
				return
			}
		}
		errc <- sc.Err()
	}()
	go func() {
		w := bufio.NewWriter(os.Stdout)
		for {
			_, b, err := c.Read(ctx)
			if err != nil {
				errc <- err
				return
			}
			w.Write(b)
			w.WriteByte('\n')
			if err := w.Flush(); err != nil {
				errc <- err
				return
			}
		}
	}()
	// Vim closes stdin to leave and sends SIGTERM when it exits. Close the
	// WebSocket properly then, or the server may not notice that we left.
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	select {
	case err = <-errc:
	case <-sig:
		err = nil
	}
	if err == nil {
		c.Close(websocket.StatusNormalClosure, "")
		return nil
	}
	// The server says why it closed in a message, so a closed connection is
	// not worth another error.
	if errors.Is(err, io.EOF) || websocket.CloseStatus(err) != -1 {
		return nil
	}
	return err
}

// list fetches /sessions next to the WebSocket endpoint given by u.
func list(u string) error {
	pu, err := url.Parse(u)
	if err != nil {
		return err
	}
	switch pu.Scheme {
	case "ws":
		pu.Scheme = "http"
	case "wss":
		pu.Scheme = "https"
	}
	pu.Path = strings.TrimSuffix(pu.Path, "/ws") + "/sessions"
	hc := &http.Client{Timeout: 10 * time.Second}
	resp, err := hc.Get(pu.String())
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%s: %s", pu, resp.Status)
	}
	_, err = io.Copy(os.Stdout, resp.Body)
	return err
}

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	switch os.Args[1] {
	case "serve":
		fs := flag.NewFlagSet("serve", flag.ExitOnError)
		addr := fs.String("addr", ":8080", "listen address")
		realIP := fs.String("real-ip-header", "", "trusted header with the client IP, such as CF-Connecting-IP")
		maxMemory := fs.Int64("max-memory", defaultMaxMemory>>20, "memory for documents, history and queues, in MB")
		fs.Parse(os.Args[2:])
		log.Fatal(serve(*addr, *realIP, *maxMemory<<20))
	case "connect":
		if len(os.Args) != 3 {
			usage()
		}
		if err := connect(os.Args[2]); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	case "list":
		if len(os.Args) != 3 {
			usage()
		}
		if err := list(os.Args[2]); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	default:
		usage()
	}
}
