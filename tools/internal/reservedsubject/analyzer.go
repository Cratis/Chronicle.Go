// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

// Package reservedsubject checks for reserved subject properties in read models.
package reservedsubject

import (
	"go/ast"
	"go/types"
	"reflect"
	"strconv"
	"strings"

	"golang.org/x/tools/go/analysis"
)

const (
	code    = "CHR0035"
	message = code + ": rename the JSON property; Chronicle reserves this name for compliance subjects"
)

// Analyzer reports declared JSON names reserved by Chronicle for compliance subjects.
var Analyzer = &analysis.Analyzer{
	Name: "chroniclereservedsubject",
	Doc:  "check directly registered Chronicle read models for reserved compliance subject properties",
	Run:  run,
}

func run(pass *analysis.Pass) (any, error) {
	models := localModels(pass)
	for _, file := range pass.Files {
		inspectRegistrations(pass, file, models)
	}
	return nil, nil
}

func localModels(pass *analysis.Pass) map[*types.Named]*ast.StructType {
	models := make(map[*types.Named]*ast.StructType)
	for _, file := range pass.Files {
		for node := range ast.Preorder(file) {
			if spec, ok := node.(*ast.TypeSpec); ok {
				indexModel(pass, models, spec)
			}
		}
	}
	return models
}

func indexModel(pass *analysis.Pass, models map[*types.Named]*ast.StructType, spec *ast.TypeSpec) {
	if spec.Assign.IsValid() || spec.TypeParams != nil {
		return
	}
	shape, ok := spec.Type.(*ast.StructType)
	if !ok {
		return
	}
	typ, ok := packageModelType(pass, spec.Name)
	if ok {
		models[typ] = shape
	}
}

func packageModelType(pass *analysis.Pass, name *ast.Ident) (*types.Named, bool) {
	named, ok := pass.TypesInfo.TypeOf(name).(*types.Named)
	if !ok || named.Obj().Parent() != pass.Pkg.Scope() {
		return nil, false
	}
	return named, named.NumMethods() == 0
}

func inspectRegistrations(pass *analysis.Pass, file *ast.File, models map[*types.Named]*ast.StructType) {
	for node := range ast.Preorder(file) {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			continue
		}
		typ := registeredModel(pass.TypesInfo, call)
		shape, exists := models[typ]
		if !exists {
			continue
		}
		delete(models, typ) // Only report each field once per package.
		for _, tag := range reservedTags(pass.TypesInfo, shape) {
			pass.Report(analysis.Diagnostic{Pos: tag.Pos(), End: tag.End(), Category: code, Message: message})
		}
	}
}

func registeredModel(info *types.Info, call *ast.CallExpr) *types.Named {
	if len(call.Args) != 1 || call.Ellipsis.IsValid() {
		return nil
	}
	instantiated, ok := ast.Unparen(call.Fun).(*ast.IndexExpr)
	if !ok || !isRegisterReadModel(info, instantiated.X) {
		return nil
	}
	argument, ok := ast.Unparen(instantiated.Index).(*ast.Ident)
	if !ok {
		return nil
	}
	named, ok := info.TypeOf(argument).(*types.Named)
	if !ok || info.ObjectOf(argument) != named.Obj() {
		return nil
	}
	return named
}

func isRegisterReadModel(info *types.Info, expr ast.Expr) bool {
	var name *ast.Ident
	switch expr := ast.Unparen(expr).(type) {
	case *ast.Ident:
		name = expr
	case *ast.SelectorExpr:
		name = expr.Sel
	default:
		return false
	}
	fn, ok := info.ObjectOf(name).(*types.Func)
	if !ok || fn.Signature().Recv() != nil {
		return false
	}
	if fn.Pkg() == nil {
		return false
	}
	return fn.Name() == "RegisterReadModel" && fn.Pkg().Path() == "github.com/cratis/chronicle.go"
}

// reservedTags gathers findings only after the complete flat model passes preflight.
func reservedTags(info *types.Info, shape *ast.StructType) []*ast.BasicLit {
	seen := make(map[string]bool)
	var tags []*ast.BasicLit
	for _, field := range shape.Fields.List {
		name, reportable, ok := property(info, field)
		if !ok {
			return nil
		}
		if name == "" {
			continue
		}
		folded := strings.ToLower(name)
		if seen[folded] {
			return nil
		}
		seen[folded] = true
		if reportable && reserved(folded) {
			tags = append(tags, field.Tag)
		}
	}
	return tags
}

func property(info *types.Info, field *ast.Field) (name string, reportable, ok bool) {
	if len(field.Names) != 1 {
		return "", false, false
	}
	typ := builtinScalar(info, field.Type)
	if typ == "" {
		return "", false, false
	}
	name, ok = explicitJSONName(field.Tag)
	if !ok {
		return "", false, false
	}
	if !field.Names[0].IsExported() || name == "-" {
		return "", false, true
	}
	reportable = name != "" && typ == "string"
	if name == "" {
		name = defaultName(field)
	}
	return name, reportable, safeName(name)
}

func builtinScalar(info *types.Info, expr ast.Expr) string {
	id, ok := ast.Unparen(expr).(*ast.Ident)
	if !ok {
		return ""
	}
	switch id.Name {
	case "string", "bool", "int":
		if info.ObjectOf(id) == types.Universe.Lookup(id.Name) {
			return id.Name
		}
	}
	return ""
}

func explicitJSONName(tag *ast.BasicLit) (string, bool) {
	if tag == nil {
		return "", true
	}
	literal, err := strconv.Unquote(tag.Value)
	if err != nil {
		return "", false
	}
	value, ok := reflect.StructTag(literal).Lookup("json")
	if !ok || literal != `json:`+strconv.Quote(value) {
		return "", false
	}
	parts := strings.Split(value, ",")
	for _, option := range parts[1:] {
		if option != "omitempty" && option != "omitzero" {
			return "", false
		}
	}
	return parts[0], true
}

func defaultName(field *ast.Field) string {
	name := field.Names[0].Name
	if field.Tag == nil && name == "ID" {
		return "Id"
	}
	return name
}

func safeName(name string) bool {
	const letters = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz_"
	if name == "Week" || !strings.ContainsRune(letters, rune(name[0])) {
		return false
	}
	return strings.IndexFunc(name[1:], func(r rune) bool {
		return !strings.ContainsRune(letters+"0123456789", r)
	}) == -1
}

func reserved(folded string) bool {
	switch folded {
	case "_subject", "__subject", "__subjects":
		return true
	default:
		return false
	}
}
