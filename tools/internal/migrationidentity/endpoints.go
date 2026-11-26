// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package migrationidentity

import (
	"go/ast"
	"go/constant"
	"go/token"
	"go/types"
	"math"
	"strconv"
	"strings"
)

const eventsPath = "github.com/cratis/chronicle.go/events"

type endpoint struct {
	handle, err *types.Var
	id          string
}

func checkedEndpoint(info *types.Info, binding, guard ast.Stmt, reusableErr *types.Var) (endpoint, bool) {
	handleName, errorName, call := twoResultCall(binding)
	if call == nil {
		return endpoint{}, false
	}
	handle := freshVar(info, handleName)
	if handle == nil {
		return endpoint{}, false
	}
	err := errorBinding(info, errorName, reusableErr)
	if err == nil || !returnsOnError(info, guard, err) {
		return endpoint{}, false
	}
	id, ok := definitionID(info, call)
	return endpoint{handle: handle, err: err, id: id}, ok
}

func freshVar(info *types.Info, name *ast.Ident) *types.Var {
	if name.Name == "_" {
		return nil
	}
	v, _ := info.Defs[name].(*types.Var)
	return v
}

func errorBinding(info *types.Info, name *ast.Ident, reusable *types.Var) *types.Var {
	if fresh := freshVar(info, name); fresh != nil {
		return fresh
	}
	if reusable != nil && info.Uses[name] == reusable {
		return reusable
	}
	return nil
}

func returnsOnError(info *types.Info, stmt ast.Stmt, err *types.Var) bool {
	guard, ok := stmt.(*ast.IfStmt)
	if !ok || !isSimpleReturnGuard(guard) {
		return false
	}
	cond, ok := guard.Cond.(*ast.BinaryExpr)
	if !ok || cond.Op != token.NEQ {
		return false
	}
	return usedObject(info, cond.X) == err && usedObject(info, cond.Y) == types.Universe.Lookup("nil")
}

func isSimpleReturnGuard(guard *ast.IfStmt) bool {
	if guard.Init != nil || guard.Else != nil {
		return false
	}
	if len(guard.Body.List) != 1 {
		return false
	}
	_, ok := guard.Body.List[0].(*ast.ReturnStmt)
	return ok
}

func definitionID(info *types.Info, call *ast.CallExpr) (string, bool) {
	applied, ok := call.Fun.(*ast.IndexExpr)
	if !ok || !isSDKCall(info, call, "Define") {
		return "", false
	}
	if !concreteNamedStruct(info.TypeOf(applied.Index)) {
		return "", false
	}
	if len(call.Args) < 1 || len(call.Args) > 2 {
		return "", false
	}
	return explicitID(info, call.Args)
}

func concreteNamedStruct(typ types.Type) bool {
	named, ok := types.Unalias(typ).(*types.Named)
	if !ok || named.TypeArgs().Len() != 0 {
		return false
	}
	_, ok = named.Underlying().(*types.Struct)
	return ok
}

func explicitID(info *types.Info, options []ast.Expr) (string, bool) {
	idOption := options[0]
	if len(options) == 2 {
		switch {
		case positiveGeneration(info, options[0]):
			idOption = options[1]
		case positiveGeneration(info, options[1]):
		default:
			return "", false
		}
	}
	id := literalID(info, idOption)
	return id, id != ""
}

func literalID(info *types.Info, option ast.Expr) string {
	literal := literalOption(info, option, "WithID", token.STRING)
	if literal == nil {
		return ""
	}
	id, err := strconv.Unquote(literal.Value)
	if err != nil {
		return ""
	}
	if strings.TrimSpace(id) == "" || strings.Contains(id, ",") {
		return ""
	}
	return id
}

func positiveGeneration(info *types.Info, option ast.Expr) bool {
	literal := literalOption(info, option, "WithGeneration", token.INT)
	if literal == nil {
		return false
	}
	generation, exact := constant.Uint64Val(info.Types[literal].Value)
	return exact && generation > 0 && generation <= math.MaxUint32
}

func literalOption(info *types.Info, expr ast.Expr, name string, kind token.Token) *ast.BasicLit {
	call, ok := expr.(*ast.CallExpr)
	if !ok || len(call.Args) != 1 {
		return nil
	}
	if !isSDKCall(info, call, name) {
		return nil
	}
	literal, ok := call.Args[0].(*ast.BasicLit)
	if !ok || literal.Kind != kind {
		return nil
	}
	return literal
}

func isSDKCall(info *types.Info, call *ast.CallExpr, name string) bool {
	if call.Ellipsis.IsValid() {
		return false
	}
	selector := sdkSelector(info, call.Fun)
	if selector == nil {
		return false
	}
	fn, ok := info.Uses[selector.Sel].(*types.Func)
	return ok && fn.Name() == name && fn.Pkg() != nil && fn.Pkg().Path() == eventsPath && fn.Signature().Recv() == nil
}

func sdkSelector(info *types.Info, expr ast.Expr) *ast.SelectorExpr {
	switch applied := expr.(type) {
	case *ast.IndexExpr:
		expr = applied.X
	case *ast.IndexListExpr:
		expr = applied.X
	}
	selector, ok := expr.(*ast.SelectorExpr)
	if !ok {
		return nil
	}
	pkg, ok := usedObject(info, selector.X).(*types.PkgName)
	if !ok || pkg.Imported().Path() != eventsPath {
		return nil
	}
	return selector
}
