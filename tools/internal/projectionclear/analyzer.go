// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

// Package projectionclear checks direct fluent projection writes that cannot hold null.
package projectionclear

import (
	"go/ast"
	"go/token"
	"go/types"
	"strings"

	"golang.org/x/tools/go/analysis"
)

const (
	projectionsPath = "github.com/cratis/chronicle.go/projections"
	code            = "CHR0048"
	message         = code + ": clear requires a nullable field with a matching descriptor; otherwise set an explicit value"
)

// Analyzer reports clears of proven nonnullable strings in a narrow fluent shape.
var Analyzer = &analysis.Analyzer{
	Name: "chronicleprojectionclear",
	Doc:  "check direct fluent Chronicle projection clears of required strings",
	Run:  run,
}

type modelShapes map[*types.TypeName]*ast.StructType

type matcher struct {
	info    *types.Info
	pkg     *types.PkgName
	structs modelShapes
}

func run(pass *analysis.Pass) (any, error) {
	structs := localStructs(pass)
	for _, file := range pass.Files {
		if pkg := canonicalImport(pass.TypesInfo, file); pkg != nil {
			matcher{info: pass.TypesInfo, pkg: pkg, structs: structs}.inspect(pass, file)
		}
	}
	return nil, nil
}

func localStructs(pass *analysis.Pass) modelShapes {
	structs := make(modelShapes)
	for _, file := range pass.Files {
		for node := range ast.Preorder(file) {
			if spec, ok := node.(*ast.TypeSpec); ok {
				indexStruct(pass, structs, spec)
			}
		}
	}
	return structs
}

func indexStruct(pass *analysis.Pass, structs modelShapes, spec *ast.TypeSpec) {
	if spec.Assign.IsValid() || spec.TypeParams != nil {
		return
	}
	shape, ok := spec.Type.(*ast.StructType)
	if !ok {
		return
	}
	named, ok := pass.TypesInfo.TypeOf(spec.Name).(*types.Named)
	if !ok || named.Obj().Parent() != pass.Pkg.Scope() {
		return
	}
	if named.NumMethods() == 0 {
		structs[named.Obj()] = shape
	}
}

func canonicalImport(info *types.Info, file *ast.File) *types.PkgName {
	for _, spec := range file.Imports {
		if spec.Name != nil {
			continue
		}
		pkg, ok := info.Implicits[spec].(*types.PkgName)
		if ok && pkg.Imported().Path() == projectionsPath {
			return pkg
		}
	}
	return nil
}

func (m matcher) inspect(pass *analysis.Pass, file *ast.File) {
	for node := range ast.Preorder(file) {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			continue
		}
		if selector := m.requiredClear(call); selector != nil {
			pass.Report(analysis.Diagnostic{Pos: selector.Pos(), End: selector.End(), Category: code, Message: message})
		}
	}
}

func (m matcher) requiredClear(call *ast.CallExpr) *ast.Ident {
	if m.function(call, "From", 3) == nil {
		return nil
	}
	body, ok := call.Args[2].(*ast.FuncLit)
	if !ok {
		return nil
	}
	parameter, model := m.callbackParameter(body)
	if parameter == nil {
		return nil
	}
	clear := singleCall(body.Body)
	selector := m.function(clear, "Clear", 2)
	if selector == nil || m.used(clear.Args[0]) != parameter {
		return nil
	}
	if !hasRequiredTarget(m.structs[model], m.info, m.target(clear.Args[1], model)) {
		return nil
	}
	return selector
}

func (m matcher) callbackParameter(body *ast.FuncLit) (types.Object, *types.TypeName) {
	params := body.Type.Params.List
	if len(params) != 1 || len(params[0].Names) != 1 {
		return nil, nil
	}
	ptr, ok := params[0].Type.(*ast.StarExpr)
	if !ok {
		return nil, nil
	}
	model := m.builderModel(ptr.X)
	if model == nil {
		return nil, nil
	}
	return m.info.Defs[params[0].Names[0]], model
}

func (m matcher) builderModel(expr ast.Expr) *types.TypeName {
	builder, ok := expr.(*ast.IndexListExpr)
	if !ok || len(builder.Indices) != 2 {
		return nil
	}
	if _, ok := m.member(builder.X, "FromBuilder").(*types.TypeName); !ok {
		return nil
	}
	model, _ := m.used(builder.Indices[0]).(*types.TypeName)
	event, _ := m.used(builder.Indices[1]).(*types.TypeName)
	if m.structs[model] == nil || m.structs[event] == nil {
		return nil
	}
	return model
}

func singleCall(body *ast.BlockStmt) *ast.CallExpr {
	if len(body.List) != 1 {
		return nil
	}
	stmt, ok := body.List[0].(*ast.ExprStmt)
	if !ok {
		return nil
	}
	call, _ := stmt.X.(*ast.CallExpr)
	return call
}

func (m matcher) target(expr ast.Expr, model *types.TypeName) string {
	path, ok := expr.(*ast.CallExpr)
	if !ok || !callArity(path, 1) {
		return ""
	}
	if !m.stringPath(path.Fun, model) {
		return ""
	}
	literal, ok := path.Args[0].(*ast.BasicLit)
	if !ok || literal.Kind != token.STRING {
		return ""
	}
	return plainName(literal.Value, `"`, `"`)
}

func (m matcher) stringPath(expr ast.Expr, model *types.TypeName) bool {
	specialized, ok := expr.(*ast.IndexListExpr)
	if !ok || len(specialized.Indices) != 2 {
		return false
	}
	if _, ok := m.member(specialized.X, "Path").(*types.Func); !ok {
		return false
	}
	return m.used(specialized.Indices[0]) == model && isString(m.info, specialized.Indices[1])
}

func (m matcher) used(expr ast.Expr) types.Object {
	if ident, ok := expr.(*ast.Ident); ok {
		return m.info.Uses[ident]
	}
	return nil
}

func (m matcher) function(call *ast.CallExpr, name string, arity int) *ast.Ident {
	if call == nil || !callArity(call, arity) {
		return nil
	}
	if _, ok := m.member(call.Fun, name).(*types.Func); !ok {
		return nil
	}
	return call.Fun.(*ast.SelectorExpr).Sel
}

func callArity(call *ast.CallExpr, n int) bool {
	return len(call.Args) == n && !call.Ellipsis.IsValid()
}

func (m matcher) member(expr ast.Expr, name string) types.Object {
	selector, ok := expr.(*ast.SelectorExpr)
	if !ok || m.used(selector.X) != m.pkg {
		return nil
	}
	obj := m.info.Uses[selector.Sel]
	if obj == m.pkg.Imported().Scope().Lookup(name) {
		return obj
	}
	return nil
}

func hasRequiredTarget(shape *ast.StructType, info *types.Info, target string) bool {
	if target == "" || strings.EqualFold(target, "id") {
		return false
	}
	seen := make(map[string]bool, len(shape.Fields.List))
	found := false
	for _, field := range shape.Fields.List {
		name := wireName(info, field)
		folded := strings.ToLower(name)
		if name == "" || seen[folded] {
			return false
		}
		seen[folded] = true
		if name == target && !strings.EqualFold(field.Names[0].Name, "id") {
			found = true
		}
	}
	return found
}

func wireName(info *types.Info, field *ast.Field) string {
	if len(field.Names) != 1 || !field.Names[0].IsExported() {
		return ""
	}
	if field.Tag == nil || !isString(info, field.Type) {
		return ""
	}
	// A lowercase-leading alphanumeric name cannot be a reserved subject or kernel token.
	return plainName(field.Tag.Value, "`json:\"", "\"`")
}

func isString(info *types.Info, expr ast.Expr) bool {
	id, ok := expr.(*ast.Ident)
	return ok && info.Uses[id] == types.Universe.Lookup("string")
}

func plainName(literal, prefix, suffix string) string {
	value, ok := strings.CutPrefix(literal, prefix)
	if !ok {
		return ""
	}
	name, ok := strings.CutSuffix(value, suffix)
	if !ok || !simpleName(name) {
		return ""
	}
	return name
}

func simpleName(name string) bool {
	if name == "" || !asciiLower(rune(name[0])) {
		return false
	}
	for _, ch := range name[1:] {
		if !asciiAlphanumeric(ch) {
			return false
		}
	}
	return true
}

func asciiAlphanumeric(ch rune) bool {
	switch {
	case asciiLower(ch):
		return true
	case ch >= 'A' && ch <= 'Z':
		return true
	default:
		return ch >= '0' && ch <= '9'
	}
}

func asciiLower(ch rune) bool {
	return ch >= 'a' && ch <= 'z'
}
