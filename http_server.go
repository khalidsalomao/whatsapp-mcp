package main

import (
	"context"
	"errors"
	"net"
	"net/http"
	"time"
)

// Serve returns as soon as Shutdown closes the listener. Wait for Shutdown
// itself before the caller releases the service and databases used by handlers.
func serveHTTP(ctx context.Context, server *http.Server, listener net.Listener) error {
	done := make(chan struct{})
	stop := context.AfterFunc(ctx, func() {
		defer close(done)
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdown); err != nil {
			_ = server.Close()
		}
	})
	err := server.Serve(listener)
	if stop() {
		_ = server.Close()
	} else {
		<-done
	}
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}
