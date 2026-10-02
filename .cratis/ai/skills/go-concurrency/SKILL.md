---
name: go-concurrency
description: Design and review Go goroutine lifetimes, shared state, channels, backpressure, subscriptions, and shutdown. Use when introducing concurrency or diagnosing races, deadlocks, or leaks.
---

# Go concurrency

Read `Documentation/project-context.md` and `.cratis/ai/rules/go.md` first.
Use `go-errors-and-context` for cancellation/error contracts and `go-testing`
for deterministic test design.

## Workflow

1. Start with a synchronous implementation. Explain why background work is
   required before adding it to a library API.
2. For every goroutine, name its owner, start, stop signal, blocking points,
   terminal error route, and join path. Include shutdown during startup failure.
3. Choose a mutex for shared state, channels for coordination, or `errgroup`
   for coordinated error propagation. Atomics do not make multi-field invariants
   atomic. `WaitGroup.Go` needs Go 1.25 and does not collect errors.
4. Bound concurrency, queues, buffering, and reconnect attempts. Define whether
   overload blocks, rejects, drops, or disconnects; never silently lose events.
5. Review all slice/map/buffer aliases, calls into user code, and resource owners.
   No callbacks under internal locks; never copy used locks or return pooled data.
6. Check cancellation at sends, receives, retry waits, and admission points.
   `errgroup.SetLimit` can block `Go`; it is not cancelable queue admission.
7. Close channels only after every sender has finished. Avoid receiver-side
   closure and recover-from-send-panic patterns. Define shutdown/error precedence.
8. Join owned work before returning from shutdown where the contract promises
   that no callbacks remain. Test repeated close and close during blocked work.

## Small example: cancellation plus joining

An illustrative synchronous runner: the owner joins its worker before returning.
`handle` must honor context; it runs serially and must not outlive its invocation.

```go
package worker

import "context"

// Run consumes values until input closes, handling fails, or ctx is canceled.
func Run(ctx context.Context, input <-chan int,
	handle func(context.Context, int) error,
) error {
	result := make(chan error, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-ctx.Done():
				result <- ctx.Err()
				return
			case value, ok := <-input:
				if !ok {
					result <- nil
					return
				}
				if err := ctx.Err(); err != nil {
					result <- err
					return
				}
				if err := handle(ctx, value); err != nil {
					result <- err
					return
				}
			}
		}
	}()
	err := <-result
	<-done
	return err
}
```

This demonstrates a completion protocol, not a reason to spawn this worker in
production: the same loop should normally run synchronously. Never return on
`ctx.Done()` while leaving owned work running without an explicit join contract.
Select does not prioritize cancellation; specify race outcomes instead of
promising that no value can be observed concurrently with cancellation.

## Verify and stop conditions

Test normal completion, failure, cancellation while blocked, empty input, and
shutdown. Use channels/joins or Go 1.25+ `testing/synctest`, not guessed sleeps.
Run `go test -race ./...` through the repository's gate. Stop if ownership or
termination cannot be explained; increasing timeouts is not a leak fix.

## References and attribution

- [Go review comments](https://go.dev/wiki/CodeReviewComments)
- [Context](https://pkg.go.dev/context) and [synctest](https://pkg.go.dev/testing/synctest)
- [Uber goroutine guidance](https://github.com/uber-go/guide/blob/master/style.md):
  Uber Technologies, Inc., [Apache-2.0](https://github.com/uber-go/guide/blob/master/LICENSE).
  Lifecycle ideas are paraphrased; the example and workflow are original.
