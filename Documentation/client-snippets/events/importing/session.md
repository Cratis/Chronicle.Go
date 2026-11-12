```go
import (
    "context"
    "fmt"

    "github.com/cratis/chronicle.go/events"
    "github.com/cratis/chronicle.go/eventsequences"
)

type LegacyOrderPlaced struct {
    OrderNumber string `json:"orderNumber"`
}

type LegacyOrder struct{ OrderID, Number string }

func migrateOrders(ctx context.Context, eventLog *eventsequences.Sequence, batches [][]LegacyOrder) (eventsequences.ImportResult, error) {
    session, err := eventLog.BeginImport(ctx, eventsequences.WithMaxChunksInFlight(4))
    if err != nil {
        return eventsequences.ImportResult{}, err
    }
    fmt.Println("Importing as", session.ID())

    for _, batch := range batches {
        entries := make([]eventsequences.Entry, len(batch))
        for i, order := range batch {
            entries[i] = eventsequences.Entry{Source: events.SourceID(order.OrderID), Event: LegacyOrderPlaced{OrderNumber: order.Number}}
        }
        if err := session.Add(ctx, eventsequences.ImportEntries(entries)); err != nil {
            return eventsequences.ImportResult{ImportID: session.ID()}, err
        }
    }

    return session.Complete(ctx)
}
```
