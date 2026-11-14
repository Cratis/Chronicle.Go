// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle_test

import (
	"os"
	"strings"
	"testing"
	"time"

	"github.com/cratis/chronicle.go/graphs"
	"github.com/cratis/chronicle.go/serialization"
)

// The nodes and the edge of the Organization graph the documentation declares.
type viewDocPerson struct {
	ID   string `json:"id" chronicle:"key"`
	Name string `json:"name"`
}

type viewDocGroup struct {
	ID    string `json:"id" chronicle:"key"`
	Title string `json:"title"`
}

type viewDocMemberOf struct {
	Since time.Time `json:"since"`
}

// graph-views:start
// GroupMembership is read from a group and the MEMBER_OF edge that led to it.
type GroupMembership struct {
	ID    string    `json:"id"`
	Title string    `json:"title"`
	Since time.Time `json:"since"`
}

// PersonWithGroups is read from a person and the groups the person is a member of.
type PersonWithGroups struct {
	ID     string            `json:"id"`
	Name   string            `json:"name"`
	Groups []GroupMembership `json:"groups"`
}

func organizationWithViews(person, group *graphs.Node, memberOf *graphs.Edge) (graphs.Declaration, error) {
	groupMembership := graphs.NewView[GroupMembership](group).
		Property("id").
		Property("title").
		EdgeProperty("since")
	personWithGroups := graphs.NewView[PersonWithGroups](person).
		Property("id").
		Property("name").
		FollowsMany("groups", memberOf, groupMembership)
	return graphs.New("Organization", person, group, memberOf).
		WithViews(personWithGroups, groupMembership).
		Build()
}

// graph-views:end

func TestGraphViewsSnippetMatchesCompiledExample(t *testing.T) {
	checkProjectionSnippets(t, "example_graph_views_test.go", "Documentation/client-snippets/projections/graph/views.md", []string{"graph-views"})
	snippet, err := os.ReadFile("Documentation/client-snippets/projections/graph/views.md")
	if err != nil {
		t.Fatal(err)
	}
	page, err := os.ReadFile("Documentation/projections/graphs.md")
	if err != nil {
		t.Fatal(err)
	}
	body := strings.TrimSuffix(strings.TrimPrefix(string(snippet), "```go\n"), "```\n") // the page and the snippet indent with spaces
	if !strings.Contains(string(page), body) {
		t.Fatal("the graph page drifted from the projections/graph/views snippet")
	}
}

func TestGraphViewsExampleDeclaresTheViews(t *testing.T) {
	person := graphs.NewNode[viewDocPerson]().Labeled("Person")
	group := graphs.NewNode[viewDocGroup]().Labeled("Group")
	memberOf := graphs.NewEdge[viewDocMemberOf](person, group).Named("MEMBER_OF")
	declaration, err := organizationWithViews(person, group, memberOf)
	if err != nil {
		t.Fatal(err)
	}
	definition, err := declaration.ViewDefinition(serialization.PreservePropertyNames)
	if err != nil {
		t.Fatal(err)
	}
	if len(definition.Views) != 2 || definition.Views[0].Name != "GroupMembership" || definition.Views[1].Name != "PersonWithGroups" {
		t.Fatalf("views = %+v", definition.Views)
	}
	groups := definition.Views[1].Members[2]
	if groups.Kind != graphs.ViewRelation || groups.Edge != "MEMBER_OF" || groups.Direction != graphs.ViewOutgoing || groups.Cardinality != graphs.ViewMany || groups.View != "GroupMembership" {
		t.Fatalf("groups = %+v", groups)
	}
}
