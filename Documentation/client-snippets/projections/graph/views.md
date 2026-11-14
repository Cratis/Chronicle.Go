```go
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
```
