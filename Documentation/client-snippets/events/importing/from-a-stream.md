```go
import (
    "context"
    "fmt"
    "iter"
    "time"

    "github.com/cratis/chronicle.go/events"
    "github.com/cratis/chronicle.go/eventsequences"
)

type LegacyCustomerRegistered struct {
    Name string `json:"name"`
}

type LegacyRow struct {
    EventID, CustomerID, Name string
    At                        time.Time
    Position                  uint64
}

func migrateCustomers(ctx context.Context, eventLog *eventsequences.Sequence, rows iter.Seq2[LegacyRow, error]) (eventsequences.ImportResult, error) {
    return eventLog.Import(ctx, toEvents(rows),
        eventsequences.WithOnInvalid(eventsequences.RejectInvalidEvents),
        eventsequences.WithImportProgress(func(progress eventsequences.ImportProgress) {
            fmt.Printf("%s: %d committed\n", progress.State, progress.EventsCommitted)
        }))
}

func toEvents(rows iter.Seq2[LegacyRow, error]) iter.Seq2[eventsequences.Entry, error] {
    return func(yield func(eventsequences.Entry, error) bool) {
        for row, err := range rows {
            if err != nil {
                yield(eventsequences.Entry{}, err)
                return
            }
            position := events.SequenceNumber(row.Position)
            if !yield(eventsequences.Entry{
                Source:               events.SourceID(row.CustomerID),
                Event:                LegacyCustomerRegistered{Name: row.Name},
                Occurred:             &row.At,
                IdempotencyKey:       eventsequences.IdempotencyKey(row.EventID),
                SourceSequenceNumber: &position,
            }, nil) {
                return
            }
        }
    }
}
```
