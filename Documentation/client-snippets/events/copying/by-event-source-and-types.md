```go
import (
    "context"

    "github.com/cratis/chronicle.go/events"
    "github.com/cratis/chronicle.go/eventsequences"
    "github.com/cratis/chronicle.go/jobs"
)

func copyAccountOpenings(ctx context.Context, eventLog *eventsequences.Sequence, source, target events.SourceID) (jobs.ID, error) {
    return eventLog.Copy(ctx, source, target, "account-opened", "account-renamed")
}
```
