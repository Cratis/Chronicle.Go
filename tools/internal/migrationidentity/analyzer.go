// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

// Package migrationidentity checks explicitly declared Chronicle migration endpoint IDs.
package migrationidentity

import (
	"go/ast"
	"go/token"
	"go/types"

	"golang.org/x/tools/go/analysis"
)

const (
	code    = "CHR0037"
	message = code + ": migration endpoints have different event type IDs; derive the previous generation from the current event type"
)

// Analyzer reports direct migrations whose checked endpoint declarations prove different IDs.
var Analyzer = &analysis.Analyzer{
	Name: "chroniclemigrationidentity",
	Doc:  "check explicit event type IDs in directly declared Chronicle migrations",
	Run:  run,
}

func run(pass *analysis.Pass) (any, error) {
	for _, file := range pass.Files {
		for node := range ast.Preorder(file) {
			if block, ok := node.(*ast.BlockStmt); ok {
				inspectBlock(pass, block.List)
			}
		}
	}
	return nil, nil
}

func inspectBlock(pass *analysis.Pass, stmts []ast.Stmt) {
	const windowSize = 5
	for i := range len(stmts) - windowSize + 1 {
		callee := migrationCallee(pass.TypesInfo, stmts[i:i+windowSize])
		if callee != nil {
			pass.Report(analysis.Diagnostic{Pos: callee.Pos(), End: callee.End(), Category: code, Message: message})
		}
	}
}

func migrationCallee(info *types.Info, stmts []ast.Stmt) *ast.Ident {
	call := migrationCall(info, stmts[4])
	if call == nil {
		return nil
	}
	first, ok := checkedEndpoint(info, stmts[0], stmts[1], nil)
	if !ok {
		return nil
	}
	second, ok := checkedEndpoint(info, stmts[2], stmts[3], first.err)
	if !ok || first.id == second.id {
		return nil
	}
	if first.handle == second.handle || !matchesEndpoints(info, call, first.handle, second.handle) {
		return nil
	}
	return sdkSelector(info, call.Fun).Sel
}

func migrationCall(info *types.Info, stmt ast.Stmt) *ast.CallExpr {
	_, _, call := twoResultCall(stmt)
	if call == nil || len(call.Args) != 3 {
		return nil
	}
	if !isSDKCall(info, call, "DefineMigration") {
		return nil
	}
	if _, ok := call.Args[2].(*ast.Ident); !ok {
		return nil
	}
	return call
}

func matchesEndpoints(info *types.Info, call *ast.CallExpr, first, second *types.Var) bool {
	up, prev := usedObject(info, call.Args[0]), usedObject(info, call.Args[1])
	return (up == first && prev == second) || (up == second && prev == first)
}

// Require plain identifier destinations: evaluating an index on the left-hand
// side could change a handle before the migration call evaluates its arguments.
func twoResultCall(stmt ast.Stmt) (first, second *ast.Ident, call *ast.CallExpr) {
	assign, ok := stmt.(*ast.AssignStmt)
	if !ok || !hasTwoResultShape(assign) {
		return nil, nil, nil
	}
	first, firstOK := assign.Lhs[0].(*ast.Ident)
	second, secondOK := assign.Lhs[1].(*ast.Ident)
	if !firstOK || !secondOK {
		return nil, nil, nil
	}
	call, _ = assign.Rhs[0].(*ast.CallExpr)
	return first, second, call
}

func hasTwoResultShape(assign *ast.AssignStmt) bool {
	switch assign.Tok {
	case token.DEFINE, token.ASSIGN:
		return len(assign.Lhs) == 2 && len(assign.Rhs) == 1
	default:
		return false
	}
}

func usedObject(info *types.Info, expr ast.Expr) types.Object {
	id, ok := expr.(*ast.Ident)
	if !ok {
		return nil
	}
	return info.Uses[id]
}
