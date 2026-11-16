package main

import (
	"context"
	"errors"
)

func (b *bridge) startPair() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.client.Store.ID != nil {
		return errors.New("already paired; use status to check the connection")
	}
	if b.pairActive {
		return nil
	}
	b.pairActive = true
	b.pair = pairing{State: "starting"}
	go b.pairAttempt()
	return nil
}

func (b *bridge) pairAttempt() {
	// Only this worker owns the attempt. It completes cleanup before releasing
	// pairActive, so a retry cannot be affected by the previous attempt's events.
	state := pairing{State: "timeout"}
	defer func() { b.mu.Lock(); b.pair = state; b.pairActive = false; b.mu.Unlock() }()
	qrCtx, cancel := context.WithCancel(b.ctx)
	defer cancel()
	b.client.Disconnect()
	qr, err := b.client.GetQRChannel(qrCtx)
	if err != nil {
		state = pairing{State: "pairing-failed"}
		b.recordError("Could not start pairing")
		return
	}
	if err = b.client.ConnectContext(b.ctx); err != nil {
		cancel()
		b.client.Disconnect()
		// whatsmeow does not close its QR channel or unregister its listener if
		// the initial dial fails before any QR event. This disconnected, unpaired
		// client has only our main handler plus that temporary QR listener.
		b.client.RemoveEventHandlers()
		b.client.AddEventHandler(b.event)
		state = pairing{State: "connection-failed"}
		b.recordError("Pairing connection failed; run pair again to retry")
		return
	}
	for {
		select {
		case <-b.ctx.Done():
			b.client.Disconnect()
			state = pairing{State: "cancelled"}
			return
		case item, ok := <-qr:
			if !ok {
				if state.State == "code" || state.State == "starting" {
					state = pairing{State: "timeout"}
				}
				return
			}
			state = pairing{State: item.Event, Code: item.Code}
			b.mu.Lock()
			b.pair = state
			b.mu.Unlock()
		}
	}
}
