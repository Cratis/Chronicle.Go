// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package graphs

// NodeFromOption configures how an event creates or changes a node.
type NodeFromOption interface{ applyNodeFrom(*nodeFrom) }

// NodeRemovalOption configures how an event deletes a node.
type NodeRemovalOption interface{ applyNodeRemoval(*nodeRemoval) }

// EdgeFromOption configures how an event creates or changes an edge.
type EdgeFromOption interface{ applyEdgeFrom(*edgeFrom) }

// EdgeRemovalOption configures how an event deletes an edge.
type EdgeRemovalOption interface{ applyEdgeRemoval(*edgeRemoval) }

// KeyOption is accepted by Node.From and Node.RemovedWith.
type KeyOption interface {
	NodeFromOption
	NodeRemovalOption
}

// EdgeKeyOption is accepted by Edge.From and Edge.RemovedWith.
type EdgeKeyOption interface {
	EdgeFromOption
	EdgeRemovalOption
}

// MappingOption maps a node or edge property from an event. It is accepted by
// Node.From and Edge.From.
type MappingOption interface {
	NodeFromOption
	EdgeFromOption
}

type nodeFrom struct {
	event      Event
	key        string
	keySet     bool
	updateOnly bool
	mappings   []mapping
}

type nodeRemoval struct {
	event  Event
	key    string
	keySet bool
	detach bool
}

type edgeFrom struct {
	event                       Event
	source, target, identity    string
	sourceSet, targetSet, idSet bool
	mappings                    []mapping
}

type edgeRemoval struct {
	event                       Event
	source, target, identity    string
	sourceSet, targetSet, idSet bool
}

type keyOption string

func (o keyOption) applyNodeFrom(f *nodeFrom)       { f.key, f.keySet = string(o), true }
func (o keyOption) applyNodeRemoval(r *nodeRemoval) { r.key, r.keySet = string(o), true }

// UsingKey names the event property that holds the key of the node. Without it
// the key is the event source id. The property is a serialized event path.
func UsingKey(eventProperty string) KeyOption { return keyOption(eventProperty) }

type sourceKeyOption string

func (o sourceKeyOption) applyEdgeFrom(f *edgeFrom)       { f.source, f.sourceSet = string(o), true }
func (o sourceKeyOption) applyEdgeRemoval(r *edgeRemoval) { r.source, r.sourceSet = string(o), true }

// UsingSourceKey names the event property that holds the key of the source node
// of the edge. Without it the key is the event source id.
func UsingSourceKey(eventProperty string) EdgeKeyOption { return sourceKeyOption(eventProperty) }

type targetKeyOption string

func (o targetKeyOption) applyEdgeFrom(f *edgeFrom)       { f.target, f.targetSet = string(o), true }
func (o targetKeyOption) applyEdgeRemoval(r *edgeRemoval) { r.target, r.targetSet = string(o), true }

// UsingTargetKey names the event property that holds the key of the target node
// of the edge. Without it the key is the event source id.
func UsingTargetKey(eventProperty string) EdgeKeyOption { return targetKeyOption(eventProperty) }

type identityOption string

func (o identityOption) applyEdgeFrom(f *edgeFrom)       { f.identity, f.idSet = string(o), true }
func (o identityOption) applyEdgeRemoval(r *edgeRemoval) { r.identity, r.idSet = string(o), true }

// UsingIdentity names the event property that holds the identity of the edge.
// An edge with an identifying property reads it from the event property of the
// same name unless this option names another one.
func UsingIdentity(eventProperty string) EdgeKeyOption { return identityOption(eventProperty) }

type onlyWhenExisting struct{}

func (onlyWhenExisting) applyNodeFrom(f *nodeFrom) { f.updateOnly = true }

// OnlyWhenExisting changes the node only when it already exists. The event never
// creates the node.
func OnlyWhenExisting() NodeFromOption { return onlyWhenExisting{} }

type detach struct{}

func (detach) applyNodeRemoval(r *nodeRemoval) { r.detach = true }

// Detach deletes the edges of the node together with it. A node that still has
// edges cannot be deleted without it. Edges cannot detach.
func Detach() NodeRemovalOption { return detach{} }
