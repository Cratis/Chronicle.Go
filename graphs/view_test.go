// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package graphs_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/cratis/chronicle.go/graphs"
	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/chronicle.go/serialization"
)

// Nodes and edges of the Organization graph of the shared view fixture.
type OrgPerson struct {
	PersonID string `json:"personId" chronicle:"key"`
	Name     string `json:"name"`
}
type OrgGroup struct {
	GroupID string `json:"groupId" chronicle:"key"`
	Name    string `json:"name"`
}
type OrgTag struct {
	TagID string `json:"tagId" chronicle:"key"`
}
type OrgMemberOf struct {
	Since time.Time `json:"since"`
}
type OrgPartOf struct{}
type OrgHasRole struct {
	AssignmentID string    `json:"assignmentId" chronicle:"key"`
	Role         string    `json:"role"`
	Since        time.Time `json:"since"`
}
type OrgTagged struct{}

// Views of the fixture. Their fields are what a host reads a view into.
type GroupMembership struct {
	ID    string
	Name  string
	Since time.Time
}
type GroupSummary struct {
	ID   string
	Name string
}
type GroupWithMembers struct {
	ID      string
	Name    string
	Members []MemberSummary
}
type GroupWithParent struct {
	ID     string
	Name   string
	Parent *GroupSummary
}
type MemberSummary struct {
	ID    string
	Name  string
	Since time.Time
}
type PersonRoles struct {
	ID    string
	Roles []RoleAssignment
}
type PersonWithGroups struct {
	ID     string
	Name   string
	Groups []GroupMembership
}
type RoleAssignment struct {
	ID           string
	AssignmentID string
	Role         string
}

// organizationSchema holds the nodes and edges of the fixture, so each case can
// declare its own views and graph over them.
type organizationSchema struct {
	person, group, tag                *graphs.Node
	memberOf, partOf, hasRole, tagged *graphs.Edge
}

func newOrganizationSchema() organizationSchema {
	s := organizationSchema{
		person: graphs.NewNode[OrgPerson]().Labeled("Person"),
		group:  graphs.NewNode[OrgGroup]().Labeled("Group"),
		tag:    graphs.NewNode[OrgTag]().Labeled("Tag"),
	}
	s.memberOf = graphs.NewEdge[OrgMemberOf](s.person, s.group).Named("MEMBER_OF")
	s.partOf = graphs.NewEdge[OrgPartOf](s.group, s.group).Named("PART_OF")
	s.hasRole = graphs.NewEdge[OrgHasRole](s.person, s.group).Named("HAS_ROLE")
	s.tagged = graphs.NewEdge[OrgTagged](s.group, s.tag).Named("TAGGED")
	return s
}

func (s organizationSchema) graph(views ...*graphs.View) *graphs.Graph {
	return graphs.New("Organization", s.person, s.group, s.tag, s.memberOf, s.partOf, s.hasRole, s.tagged).WithViews(views...)
}

// organizationViews declares the views of the shared fixture, in a different
// order than the fixture lists them.
func organizationViews(s organizationSchema) []*graphs.View {
	groupMembership := graphs.NewView[GroupMembership](s.group).
		Property("id", graphs.SourceProperty("groupId")).
		Property("name").
		EdgeProperty("since")
	groupSummary := graphs.NewView[GroupSummary](s.group).
		Property("id", graphs.SourceProperty("groupId")).
		Property("name")
	memberSummary := graphs.NewView[MemberSummary](s.person).
		Property("id", graphs.SourceProperty("personId")).
		Property("name").
		EdgeProperty("since")
	roleAssignment := graphs.NewView[RoleAssignment](s.group).
		Property("id", graphs.SourceProperty("groupId")).
		EdgeProperty("assignmentId").
		EdgeProperty("role")
	return []*graphs.View{
		graphs.NewView[PersonWithGroups](s.person).
			Property("id", graphs.SourceProperty("personId")).
			Property("name").
			FollowsMany("groups", s.memberOf, groupMembership),
		graphs.NewView[PersonRoles](s.person).
			Property("id", graphs.SourceProperty("personId")).
			FollowsMany("roles", s.hasRole, roleAssignment),
		graphs.NewView[GroupWithParent](s.group).
			Property("id", graphs.SourceProperty("groupId")).
			Property("name").
			FollowsOne("parent", s.partOf, groupSummary, graphs.Outgoing()),
		graphs.NewView[GroupWithMembers](s.group).
			Property("id", graphs.SourceProperty("groupId")).
			Property("name").
			FollowsMany("members", s.memberOf, memberSummary),
		groupMembership, groupSummary, memberSummary, roleAssignment,
	}
}

func TestOrganizationViewsMatchTheSharedFixture(t *testing.T) {
	s := newOrganizationSchema()
	declaration, err := graphs.New("Organization", s.person, s.group, s.memberOf, s.partOf, s.hasRole).
		WithViews(organizationViews(s)...).Build()
	if err != nil {
		t.Fatal(err)
	}
	definition, err := declaration.ViewDefinition(serialization.PreservePropertyNames)
	if err != nil {
		t.Fatal(err)
	}
	fixture, err := os.ReadFile(filepath.Join("..", "testdata", "graphs", "graph-views-organization.json"))
	if err != nil {
		t.Fatal(err)
	}
	withSchema, err := definition.CanonicalWithSchema()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(withSchema, fixture) {
		t.Fatalf("the schema-included form differs from the fixture:\n%s", withSchema)
	}
	// The default form is the fixture without its schema section.
	text := string(fixture)
	start := strings.Index(text, "  \"schema\": {\n")
	end := strings.Index(text, "\n  },\n")
	if start < 0 || end < start {
		t.Fatal("the fixture has no schema section")
	}
	want := text[:start] + text[end+len("\n  },\n"):]
	plain, err := definition.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	if string(plain) != want {
		t.Fatalf("the default form differs from the fixture without its schema:\n%s", plain)
	}
}

func TestCanonicalViewDefinitionFormat(t *testing.T) {
	s := newOrganizationSchema()
	declaration, err := s.graph(organizationViews(s)...).Build()
	if err != nil {
		t.Fatal(err)
	}
	definition, err := declaration.ViewDefinition(serialization.PreservePropertyNames)
	if err != nil {
		t.Fatal(err)
	}
	for _, form := range []func() ([]byte, error){definition.Canonical, definition.CanonicalWithSchema} {
		canonical, err := form()
		if err != nil {
			t.Fatal(err)
		}
		text := string(canonical)
		if !strings.HasSuffix(text, "}\n") || strings.HasSuffix(text, "\n\n") || strings.Contains(text, "\r") || strings.Contains(text, "\t") || strings.Contains(text, "\n\n") {
			t.Fatalf("not canonical: %q", text)
		}
		var parsed struct {
			Views []struct{ Name string } `json:"views"`
		}
		if err := json.Unmarshal(canonical, &parsed); err != nil {
			t.Fatalf("not valid JSON: %v", err)
		}
		var names []string
		for _, view := range parsed.Views {
			names = append(names, view.Name)
		}
		want := []string{"GroupMembership", "GroupSummary", "GroupWithMembers", "GroupWithParent", "MemberSummary", "PersonRoles", "PersonWithGroups", "RoleAssignment"}
		if !reflect.DeepEqual(names, want) {
			t.Fatalf("views are not sorted by name: %v", names)
		}
		again, err := form()
		if err != nil || !bytes.Equal(canonical, again) {
			t.Fatal("serialization is not deterministic")
		}
	}
}

func TestEmptyViewListsAreWrittenInline(t *testing.T) {
	definition := graphs.GraphViewDefinition{Graph: "G", Views: []graphs.ViewDefinition{{Name: "V", Root: "N"}}}
	text, err := definition.CanonicalWithSchema()
	if err != nil {
		t.Fatal(err)
	}
	want := "{\n  \"graph\": \"G\",\n  \"schema\": {\n    \"nodes\": [],\n    \"edges\": []\n  },\n  \"views\": [\n    {\n      \"name\": \"V\",\n      \"root\": \"N\",\n      \"members\": []\n    }\n  ]\n}\n"
	if string(text) != want {
		t.Fatalf("got %q", text)
	}
}

func TestViewsAreRefusedWithAStableReason(t *testing.T) {
	type tester func(s organizationSchema) *graphs.Graph
	summary := func(s organizationSchema) *graphs.View {
		return graphs.NewView[GroupSummary](s.group).Property("id", graphs.SourceProperty("groupId"))
	}
	cases := []struct {
		reason graphs.ViewReason
		graph  tester
	}{
		{graphs.ViewRootNotANode, func(s organizationSchema) *graphs.Graph {
			stray := graphs.NewNode[OrgTag]().Labeled("Stray")
			return s.graph(graphs.NewView[GroupSummary](stray).Property("tagId"))
		}},
		{graphs.ViewIsAlsoSchema, func(s organizationSchema) *graphs.Graph {
			return s.graph(graphs.NewView[OrgPerson](s.person).Property("name"))
		}},
		{graphs.DuplicateViewName, func(s organizationSchema) *graphs.Graph {
			return s.graph(summary(s), graphs.NewView[GroupWithParent](s.group).Named("GroupSummary").Property("name"))
		}},
		{graphs.UnknownNodeProperty, func(s organizationSchema) *graphs.Graph {
			return s.graph(graphs.NewView[GroupSummary](s.group).Property("nickname"))
		}},
		{graphs.UnknownEdge, func(s organizationSchema) *graphs.Graph {
			stray := graphs.NewEdge[OrgMemberOf](s.person, s.group).Named("STRAY")
			target := summary(s)
			return s.graph(graphs.NewView[PersonWithGroups](s.person).FollowsMany("groups", stray, target), target)
		}},
		{graphs.UnknownEdgeProperty, func(s organizationSchema) *graphs.Graph {
			target := graphs.NewView[GroupMembership](s.group).EdgeProperty("until")
			return s.graph(graphs.NewView[PersonWithGroups](s.person).FollowsMany("groups", s.memberOf, target), target)
		}},
		{graphs.EdgeNotIncidentOnRoot, func(s organizationSchema) *graphs.Graph {
			tag := graphs.NewView[OrgTagView](s.tag).Property("tagId")
			return s.graph(graphs.NewView[PersonWithGroups](s.person).FollowsMany("tags", s.tagged, tag), tag)
		}},
		{graphs.AmbiguousDirection, func(s organizationSchema) *graphs.Graph {
			target := summary(s)
			return s.graph(graphs.NewView[GroupWithParent](s.group).FollowsOne("parent", s.partOf, target), target)
		}},
		{graphs.TargetLabelMismatch, func(s organizationSchema) *graphs.Graph {
			wrong := graphs.NewView[MemberSummary](s.person).Property("name")
			return s.graph(graphs.NewView[PersonWithGroups](s.person).FollowsMany("groups", s.memberOf, wrong), wrong)
		}},
		{graphs.RelationTargetNotAView, func(s organizationSchema) *graphs.Graph {
			notAdded := summary(s)
			return s.graph(graphs.NewView[PersonWithGroups](s.person).FollowsMany("groups", s.memberOf, notAdded))
		}},
		{graphs.RelationTargetNotAView, func(s organizationSchema) *graphs.Graph {
			return s.graph(graphs.NewView[PersonWithGroups](s.person).FollowsMany("groups", s.memberOf, nil))
		}},
		{graphs.RecursiveView, func(s organizationSchema) *graphs.Graph {
			tree := graphs.NewView[GroupTree](s.group).Property("name")
			tree.FollowsMany("children", s.partOf, tree, graphs.Incoming())
			return s.graph(tree)
		}},
		{graphs.RecursiveView, func(s organizationSchema) *graphs.Graph {
			person := graphs.NewView[PersonWithGroups](s.person).Property("name")
			group := graphs.NewView[GroupWithMembers](s.group).Property("name")
			person.FollowsMany("groups", s.memberOf, group)
			group.FollowsMany("members", s.memberOf, person)
			return s.graph(person, group)
		}},
		{graphs.EdgePropertyViewNotUsedAsTarget, func(s organizationSchema) *graphs.Graph {
			return s.graph(graphs.NewView[GroupMembership](s.group).EdgeProperty("since"))
		}},
		{graphs.EdgePropertyViewSharedAcrossEdges, func(s organizationSchema) *graphs.Graph {
			shared := graphs.NewView[GroupMembership](s.group).EdgeProperty("since")
			return s.graph(
				graphs.NewView[PersonWithGroups](s.person).FollowsMany("groups", s.memberOf, shared),
				graphs.NewView[PersonRoles](s.person).FollowsMany("roles", s.hasRole, shared),
				shared)
		}},
	}
	for _, c := range cases {
		t.Run(string(c.reason), func(t *testing.T) {
			_, err := c.graph(newOrganizationSchema()).Build()
			if !errors.Is(err, c.reason) {
				t.Fatalf("Build() = %v, want reason %s", err, c.reason)
			}
			if reason, ok := graphs.ViewReasonOf(err); !ok || reason != c.reason {
				t.Fatalf("ViewReasonOf = %q, %v", reason, ok)
			}
			var viewErr *graphs.ViewError
			if !errors.As(err, &viewErr) || viewErr.Graph != "Organization" || viewErr.View == "" && c.reason != graphs.RelationTargetNotAView {
				t.Fatalf("ViewError = %+v", viewErr)
			}
			if !errors.Is(err, faults.ErrInvalidConfiguration) {
				t.Fatal("a refused view does not match ErrInvalidConfiguration")
			}
		})
	}
}

// OrgTagView and GroupTree are view types only the refusal cases use.
type OrgTagView struct{ TagID string }
type GroupTree struct {
	Name     string
	Children []GroupTree
}

func TestExplicitDirectionMustMatchANonSelfEdge(t *testing.T) {
	s := newOrganizationSchema()
	target := graphs.NewView[GroupSummary](s.group).Property("name")
	_, err := s.graph(graphs.NewView[PersonWithGroups](s.person).FollowsMany("groups", s.memberOf, target, graphs.Incoming()), target).Build()
	if !errors.Is(err, graphs.EdgeNotIncidentOnRoot) {
		t.Fatalf("Build() = %v", err)
	}
}

func TestViewMembersNeedDistinctNames(t *testing.T) {
	s := newOrganizationSchema()
	_, err := s.graph(graphs.NewView[GroupSummary](s.group).Property("name").Property("name")).Build()
	if !errors.Is(err, graphs.ErrInvalidGraph) {
		t.Fatalf("Build() = %v", err)
	}
	_, err = s.graph(graphs.NewView[GroupSummary](s.group).Property("name", nil)).Build()
	if !errors.Is(err, graphs.ErrInvalidGraph) {
		t.Fatalf("nil option: Build() = %v", err)
	}
	_, err = s.graph(nil).Build()
	if !errors.Is(err, graphs.ErrInvalidGraph) {
		t.Fatalf("nil view: Build() = %v", err)
	}
}

func TestViewPropertiesMatchIgnoringCaseAndFollowTheNamingPolicy(t *testing.T) {
	s := newOrganizationSchema()
	declaration, err := s.graph(graphs.NewView[GroupSummary](s.group).Property("id", graphs.SourceProperty("GROUPID")).Property("Name")).Build()
	if err != nil {
		t.Fatal(err)
	}
	definition, err := declaration.ViewDefinition(serialization.PreservePropertyNames)
	if err != nil {
		t.Fatal(err)
	}
	members := definition.Views[0].Members
	if members[0].Source != "groupId" || members[1].Source != "name" || members[1].Name != "Name" {
		t.Fatalf("members = %+v", members)
	}
}

func TestGraphsWithoutViewsAreUnchanged(t *testing.T) {
	f := newFixture(t, serialization.PreservePropertyNames)
	plain, err := compile(t, organization(f), f, serialization.PreservePropertyNames)
	if err != nil {
		t.Fatal(err)
	}
	if views := plain.Views(); views.Graph != "Organization" || len(views.Views) != 0 {
		t.Fatalf("views = %+v", views)
	}
	empty, err := compile(t, organization(f).WithViews(), f, serialization.PreservePropertyNames)
	if err != nil {
		t.Fatal(err)
	}
	if plain.Schema() != empty.Schema() || !reflect.DeepEqual(plain.Projection().String(), empty.Projection().String()) {
		t.Fatal("an empty WithViews changed the definition")
	}
}

func TestViewsDoNotChangeTheGraphProjection(t *testing.T) {
	f := newFixture(t, serialization.PreservePropertyNames)
	plain, err := compile(t, organization(f), f, serialization.PreservePropertyNames)
	if err != nil {
		t.Fatal(err)
	}
	person := graphs.NewNode[Person]().
		From(f.registered, graphs.UsingKey("personId")).
		RemovedWith(f.removedType, graphs.UsingKey("personId"), graphs.Detach())
	group := graphs.NewNode[Group]().
		From(f.createdType, graphs.UsingKey("groupId")).
		From(f.addedType, graphs.UsingKey("groupId"), graphs.Increment("memberCount")).
		From(f.leftType, graphs.UsingKey("groupId"), graphs.Decrement("memberCount"))
	memberOf := graphs.NewEdge[MemberOf](person, group).
		Named("MEMBER_OF").
		From(f.addedType, graphs.UsingSourceKey("personId"), graphs.UsingTargetKey("groupId"), graphs.SetFromContext("since", "occurred")).
		RemovedWith(f.leftType, graphs.UsingSourceKey("personId"), graphs.UsingTargetKey("groupId"))
	summary := graphs.NewView[PersonSummaryOnly](person).Property("name")
	withViews, err := compile(t, graphs.New("Organization", person, group, memberOf).WithViews(summary), f, serialization.PreservePropertyNames)
	if err != nil {
		t.Fatal(err)
	}
	if plain.Schema() != withViews.Schema() || plain.Projection().String() != withViews.Projection().String() || plain.ReadModel().String() != withViews.ReadModel().String() {
		t.Fatal("declaring views changed the wire definition")
	}
	if got := withViews.Views(); len(got.Views) != 1 || got.Views[0].Name != "PersonSummaryOnly" || got.Views[0].Members[0].Source != "name" {
		t.Fatalf("views = %+v", got)
	}
}

type PersonSummaryOnly struct{ Name string }
