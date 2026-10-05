// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package serialization

import (
	"encoding/json"
	"reflect"

	"github.com/cratis/chronicle.go/internal/contentencoding"
)

// MarshalContent is the module-private outgoing encoding bridge. Ordinary
// Marshal, schema construction and incoming decoding never invoke enrichers.
func (p *Plan) MarshalContent(request contentencoding.Request[EventContent]) ([]byte, error) {
	properties, err := p.contentProperties(request.Value)
	if err != nil {
		if request.Failed != nil {
			failure, _ := err.(*contentFailure)
			err = request.Failed(-1, true, failure != nil && failure.panicked)
		}
		return nil, err
	}
	for i, provider := range request.Providers {
		editor := &contentEditor{active: true, fields: p.root.fields, properties: properties, readOnly: request.ReadOnly, immutable: request.Immutable}
		err = invokeContentProvider(editor, provider)
		if err != nil {
			if request.Failed != nil {
				failure, _ := editor.failure.(*contentFailure)
				err = request.Failed(i, editor.failure != nil, failure != nil && failure.panicked)
			}
			return nil, err
		}
		properties = editor.properties
	}
	data := []byte{'{'}
	for i, property := range properties {
		if i != 0 {
			data = append(data, ',')
		}
		data = appendJSONString(data, property.name)
		data = append(data, ':')
		data = append(data, property.data...)
	}
	return append(data, '}'), nil
}
func invokeContentProvider(editor *contentEditor, provider func(*EventContent) error) (err error) {
	defer func() {
		editor.mu.Lock()
		editor.active = false
		if err == nil {
			err = editor.failure
		}
		editor.mu.Unlock()
	}()
	return provider(&EventContent{editor: editor})
}
func (p *Plan) contentProperties(value any) ([]contentProperty, error) {
	v := reflect.ValueOf(value)
	if v.IsValid() && v.Kind() == reflect.Pointer && !v.IsNil() {
		v = v.Elem()
	}
	if !v.IsValid() || v.Type() != p.typ {
		return nil, errContent
	}
	state := &encodeState{active: map[valueVisit]bool{}, privateCallbacks: true}
	encoded, err := p.root.encode(v, false, state, 0)
	if err != nil {
		return nil, &contentFailure{panicked: state.panicked}
	}
	object, ok := encoded.(orderedObject)
	if !ok {
		return nil, errContent
	}
	result := make([]contentProperty, len(object))
	for i, property := range object {
		data, err := json.Marshal(property.value)
		if err != nil {
			return nil, err
		}
		data, err = escapeJSONStrings(data)
		if err != nil {
			return nil, err
		}
		result[i] = contentProperty{property.name, data}
	}
	return result, nil
}
