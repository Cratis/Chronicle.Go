// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"
	"testing"
	"testing/synctest"
)

func TestGenerationRegistrationCloseAndShutdownCancelAndJoin(t *testing.T) {
	for _, graceful := range []bool{false, true} {
		t.Run(map[bool]string{false: "close", true: "shutdown"}[graceful], func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				client, _ := memoryLifecycleClient(t, nil)
				if err := client.Connect(t.Context()); err != nil {
					t.Fatal(err)
				}
				client.mu.Lock()
				g := client.current
				client.mu.Unlock()
				canceled, release := make(chan struct{}), make(chan struct{})
				if err := client.startRegistration(g, func(ctx context.Context) {
					<-ctx.Done()
					close(canceled)
					<-release
				}); err != nil {
					t.Fatal(err)
				}
				closed := make(chan error, 1)
				go func() {
					if graceful {
						closed <- client.Shutdown(t.Context())
					} else {
						closed <- client.Close()
					}
				}()
				<-canceled
				synctest.Wait()
				select {
				case err := <-closed:
					t.Fatal("cleanup returned before registration joined", err)
				default:
				}
				close(release)
				if err := <-closed; err != nil {
					t.Fatal(err)
				}
				if err := client.startRegistration(g, func(context.Context) { t.Error("admitted after close") }); err != ErrClosed {
					t.Fatal("closed admission", err)
				}
			})
		})
	}
}
