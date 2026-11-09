// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

// Command graphs projects events into a property graph of people and groups.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/graphs"
	"github.com/cratis/chronicle.go/metadata"
	"github.com/cratis/chronicle.go/observation"
)

// graph-events:start
// The events say what happened. Nodes and edges are what they leave behind.
type PersonRegistered struct {
	PersonID string `json:"personId"`
	Name     string `json:"name"`
}

type PersonRemoved struct {
	PersonID string `json:"personId"`
}

type GroupCreated struct {
	GroupID string `json:"groupId"`
	Title   string `json:"title"`
}

type MemberJoined struct {
	PersonID string `json:"personId"`
	GroupID  string `json:"groupId"`
}

type MemberLeft struct {
	PersonID string `json:"personId"`
	GroupID  string `json:"groupId"`
}

// graph-events:end

// graph-model:start
// Person is a node. The field tagged key identifies it.
type Person struct {
	ID   string `json:"id" chronicle:"key"`
	Name string `json:"name"`
}

// Group is a node with a counter that follows the members.
type Group struct {
	ID          string `json:"id" chronicle:"key"`
	Title       string `json:"title"`
	MemberCount int    `json:"memberCount"`
}

// MemberOf is an edge. Its end nodes are not properties; the graph names them.
type MemberOf struct {
	Since time.Time `json:"since"`
}

func declarations() (*chronicle.Registry, error) {
	registry := chronicle.NewRegistry()
	registered, err := chronicle.RegisterEvent[PersonRegistered](registry, events.WithID("person-registered"))
	if err != nil {
		return nil, err
	}
	removed, err := chronicle.RegisterEvent[PersonRemoved](registry, events.WithID("person-removed"))
	if err != nil {
		return nil, err
	}
	created, err := chronicle.RegisterEvent[GroupCreated](registry, events.WithID("group-created"))
	if err != nil {
		return nil, err
	}
	joined, err := chronicle.RegisterEvent[MemberJoined](registry, events.WithID("member-joined"))
	if err != nil {
		return nil, err
	}
	left, err := chronicle.RegisterEvent[MemberLeft](registry, events.WithID("member-left"))
	if err != nil {
		return nil, err
	}
	person := graphs.NewNode[Person]().
		From(registered, graphs.UsingKey("personId")).
		RemovedWith(removed, graphs.UsingKey("personId"), graphs.Detach())
	group := graphs.NewNode[Group]().
		From(created, graphs.UsingKey("groupId")).
		From(joined, graphs.UsingKey("groupId"), graphs.Increment("memberCount")).
		From(left, graphs.UsingKey("groupId"), graphs.Decrement("memberCount"))
	memberOf := graphs.NewEdge[MemberOf](person, group).
		Named("MEMBER_OF").
		From(joined,
			graphs.UsingSourceKey("personId"),
			graphs.UsingTargetKey("groupId"),
			graphs.SetFromContext("since", "occurred")).
		RemovedWith(left, graphs.UsingSourceKey("personId"), graphs.UsingTargetKey("groupId"))
	declaration, err := graphs.New("Organization", person, group, memberOf).Build()
	if err != nil {
		return nil, err
	}
	return registry, registry.AddGraph(declaration)
}

// graph-model:end

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := run(ctx); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context) (err error) {
	registry, err := declarations()
	if err != nil {
		return err
	}
	endpoint := os.Getenv("CHRONICLE_INTEGRATION_CONNECTION_STRING")
	if endpoint == "" {
		endpoint = "chronicle://localhost:35000"
	}
	// Development only: validated TLS and production credentials belong elsewhere.
	// The in-memory graph sink needs no Neo4j; leave the default for Neo4j.
	client, err := chronicle.Dial(ctx, chronicle.WithConnectionString(endpoint), chronicle.WithDevelopmentDefaults(),
		chronicle.WithRegistry(registry), chronicle.WithDefaultGraphSinkType(graphs.InMemoryGraph))
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, client.Close()) }()
	id, err := metadata.NewCorrelationID()
	if err != nil {
		return err
	}
	store, err := client.EventStore(ctx, chronicle.StoreName("graphs-"+id.String()))
	if err != nil {
		return err
	}
	facts := []struct {
		source events.SourceID
		event  any
	}{
		{"ada", PersonRegistered{PersonID: "ada", Name: "Ada"}},
		{"chess", GroupCreated{GroupID: "chess", Title: "Chess club"}},
		{"ada", MemberJoined{PersonID: "ada", GroupID: "chess"}},
	}
	for _, fact := range facts {
		appended, err := store.EventLog().Append(ctx, fact.source, fact.event)
		if err != nil {
			return err
		}
		if err = appended.Err(); err != nil {
			return err
		}
	}
	wait, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		info, err := store.Observers().Get(wait, "Organization", events.EventLog)
		if err != nil {
			return err
		}
		if info != nil && info.RunningState() == observation.Active && info.HandledEventCount() >= uint64(len(facts)) {
			fmt.Printf("Organization graph: %d events projected\n", info.HandledEventCount())
			return nil
		}
		select {
		case <-wait.Done():
			return wait.Err()
		case <-ticker.C:
		}
	}
}
