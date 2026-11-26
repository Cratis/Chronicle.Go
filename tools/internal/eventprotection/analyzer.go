// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

// Package eventprotection detects directly conflicting Chronicle event field tags.
package eventprotection

import (
	"go/ast"
	"go/types"
	"reflect"
	"slices"
	"strconv"
	"strings"

	"github.com/cratis/chronicle.go/declarations"
	"golang.org/x/tools/go/analysis"
)

const (
	sdkPath = "github.com/cratis/chronicle.go"
	code    = "CHR0053"
	message = code + ": choose either pii or encrypted for this field; remove the other directive"
)

// Analyzer reports incompatible protection on proven event field declarations.
var Analyzer = &analysis.Analyzer{
	Name: "chronicleprotection",
	Doc:  "check directly declared Chronicle event fields for conflicting pii and encrypted directives",
	Run:  run,
}

func run(pass *analysis.Pass) (any, error) {
	structs := localStructs(pass)
	for _, file := range pass.Files {
		for node := range ast.Preorder(file) {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				continue
			}
			typ := declaredEventType(pass.TypesInfo, call)
			decl, ok := structs[typ]
			if !ok {
				continue
			}
			delete(structs, typ) // Repeated declarations cannot re-report the same tag.
			inspectFields(pass, decl)
		}
	}
	return nil, nil
}

func localStructs(pass *analysis.Pass) map[*types.Named]*ast.StructType {
	structs := make(map[*types.Named]*ast.StructType)
	for _, file := range pass.Files {
		indexFileStructs(pass.TypesInfo, file, structs)
	}
	return structs
}

func indexFileStructs(info *types.Info, file *ast.File, structs map[*types.Named]*ast.StructType) {
	for _, decl := range file.Decls {
		group, ok := decl.(*ast.GenDecl)
		if !ok {
			continue
		}
		for _, spec := range group.Specs {
			indexStruct(info, spec, structs)
		}
	}
}

func indexStruct(info *types.Info, spec ast.Spec, structs map[*types.Named]*ast.StructType) {
	def, ok := spec.(*ast.TypeSpec)
	if !ok || def.TypeParams != nil {
		return
	}
	shape, ok := def.Type.(*ast.StructType)
	if !ok || slices.ContainsFunc(shape.Fields.List, embeddedField) {
		return
	}
	named, ok := info.TypeOf(def.Name).(*types.Named)
	// Alias types are not named declarations. NumMethods includes pointer receivers.
	if !ok || named.NumMethods() != 0 {
		return
	}
	structs[named] = shape
}

func embeddedField(field *ast.Field) bool { return len(field.Names) == 0 }

func declaredEventType(info *types.Info, call *ast.CallExpr) *types.Named {
	instantiation, ok := call.Fun.(*ast.IndexExpr)
	if !ok || call.Ellipsis.IsValid() {
		return nil
	}
	if !isSDKDeclaration(functionObject(info, instantiation.X), len(call.Args)) {
		return nil
	}
	named, _ := info.TypeOf(instantiation.Index).(*types.Named)
	return named
}

func functionObject(info *types.Info, expr ast.Expr) *types.Func {
	var name *ast.Ident
	switch expr := expr.(type) {
	case *ast.Ident:
		name = expr
	case *ast.SelectorExpr:
		name = expr.Sel
	default:
		return nil
	}
	fn, _ := info.ObjectOf(name).(*types.Func)
	return fn
}

func isSDKDeclaration(fn *types.Func, argc int) bool {
	if fn == nil || fn.Pkg() == nil {
		return false
	}
	if fn.Signature().Recv() != nil {
		return false
	}
	switch fn.Pkg().Path() {
	case sdkPath:
		return fn.Name() == "RegisterEvent" && argc == 1
	case sdkPath + "/events":
		return fn.Name() == "Define" && argc == 0
	default:
		return false
	}
}

func inspectFields(pass *analysis.Pass, decl *ast.StructType) {
	for _, field := range decl.Fields.List {
		if !simpleStringField(pass.TypesInfo, field) || !conflictingTag(field.Tag.Value) {
			continue
		}
		pass.Report(analysis.Diagnostic{Pos: field.Tag.Pos(), End: field.Tag.End(), Category: code, Message: message})
	}
}

func simpleStringField(info *types.Info, field *ast.Field) bool {
	return field.Tag != nil && len(field.Names) == 1 && field.Names[0].IsExported() && info.TypeOf(field.Type) == types.Typ[types.String]
}

func conflictingTag(literal string) bool {
	decoded, err := strconv.Unquote(literal)
	if err != nil {
		return false
	}
	tag := reflect.StructTag(decoded)
	metadata := tag.Get("chronicle")
	if metadata == "" || !supportedJSONTag(tag.Get("json")) {
		return false
	}
	directives, err := declarations.Parse(declarations.V1, metadata)
	if err != nil || declarations.Validate(declarations.Event, directives) != nil {
		return false
	}
	return conflictingProtection(directives)
}

func supportedJSONTag(tag string) bool {
	parts := strings.Split(tag, ",")
	if parts[0] == "-" {
		return false
	}
	for _, option := range parts[1:] {
		if option != "omitempty" && option != "omitzero" {
			return false
		}
	}
	return true
}

func conflictingProtection(directives []declarations.Directive) bool {
	pii, encrypted := 0, 0
	for _, directive := range directives {
		switch directive.Name {
		case "pii":
			pii++
		case "encrypted":
			encrypted++
		case "compliance-details":
			return false // The runtime rejects this with encrypted before the conjunction.
		}
	}
	return pii == 1 && encrypted == 1
}
