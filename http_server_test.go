package main

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"
)

func TestHTTPShutdownWaitsForActiveRequest(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	release := make(chan struct{})
	releaseRequest := sync.OnceFunc(func() { close(release) })
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(started)
		<-release
		fmt.Fprint(w, "completed")
	})}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	var serveErr error
	go func() {
		serveErr = serveHTTP(ctx, server, listener)
		close(done)
	}()
	t.Cleanup(func() {
		releaseRequest()
		cancel()
		server.Close()
		<-done
	})
	requestDone := make(chan error, 1)
	go func() {
		client := &http.Client{Timeout: 5 * time.Second}
		response, err := client.Get("http://" + listener.Addr().String())
		if err == nil {
			body, readErr := io.ReadAll(response.Body)
			response.Body.Close()
			if readErr != nil || string(body) != "completed" {
				err = fmt.Errorf("request lost its response: %q, %v", body, readErr)
			}
		}
		requestDone <- err
	}()
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("request did not reach the server")
	}
	cancel()
	select {
	case <-done:
		t.Fatal("server returned before its active request completed")
	case <-time.After(100 * time.Millisecond):
	}
	releaseRequest()
	if err := <-requestDone; err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
		if serveErr != nil {
			t.Fatal(serveErr)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("server did not finish shutdown after the request completed")
	}
}
