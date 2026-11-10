```go
import (
    "context"

    "github.com/cratis/chronicle.go/events"
    "github.com/cratis/chronicle.go/eventsequences"
)

type LegacyInvoiceIssued struct {
    Number string `json:"number"`
}

type LegacyInvoice struct{ InvoiceID, Number string }

func resumeInvoices(ctx context.Context, eventLog *eventsequences.Sequence, importID eventsequences.ImportID, source []LegacyInvoice) (eventsequences.ImportResult, error) {
    session, err := eventLog.ReattachImport(ctx, importID)
    if err != nil {
        return eventsequences.ImportResult{}, err
    }

    // The kernel already holds the first EventsAcknowledged events; continue with the rest.
    remaining := source[min(session.EventsAcknowledged(), int64(len(source))):]
    entries := make([]eventsequences.Entry, len(remaining))
    for i, invoice := range remaining {
        entries[i] = eventsequences.Entry{Source: events.SourceID(invoice.InvoiceID), Event: LegacyInvoiceIssued{Number: invoice.Number}}
    }
    if err := session.Add(ctx, eventsequences.ImportEntries(entries)); err != nil {
        return eventsequences.ImportResult{ImportID: importID}, err
    }
    return session.Complete(ctx)
}
```
