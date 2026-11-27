# Multiplexed observation and registration status

## Multiplexed observation

When the kernel advertises the multiplexed-observation capability, every reactor and reducer of a
client shares one bidirectional gRPC stream instead of opening a call per observer. Events are
delivered to each observer in order, a slow handler does not block the others, and a sequence gap
fails the call and re-registers the observers. Against a kernel without the capability the client
keeps opening one call per observer.

Multiplexing is on by default. Turn it off with `WithMultiplexedObservation(false)`:

```go
client, err := chronicle.NewClient(connectionString, chronicle.WithMultiplexedObservation(false))
```

## Registration status

After registering projections the client polls the kernel's registration status for this process
until the kernel has applied them. A failed application is returned as `ErrRegistrationNotApplied`.
A kernel without registration status (UNIMPLEMENTED) is not waited for, and a busy kernel
(UNAVAILABLE) is polled again. The wait is bounded to five minutes; afterwards the kernel keeps
applying the registration.

## Kernel status

The keep-alive carries the kernel status. The client logs a warning when the kernel reports
degraded or recovering and an informational message when it accepts requests again.

## Accepted outcomes

`FastForward`, `RetryPartition` and `ClearPartitionQuarantine` can report the additive outcomes
`FastForwardAccepted`, `RecoveryAccepted` and `QuarantineAccepted`: the kernel accepted the request
and applies it in the background.
