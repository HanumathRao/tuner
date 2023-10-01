package main

import (
	"fmt"
	"os"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	_ "github.com/pingcap/tidb/parser/test_driver"
	_ "github.com/pingcap/tidb/types/parser_driver"
)

type typeAnalysis struct {
	typesList []string
}

func (v *typeAnalysis) analyzeTypes(node ast.Node) (string, bool)  {
	switch node.(type) {
	case *ast.BetweenExpr, *ast.BinaryOperationExpr:
		return "Filter", true
	case *ast.OrderByClause:
		return "OrderBy", true
	case *ast.GroupByClause:
		return "GroupBy", true
	case *ast.Join:
		return "Join", true
	case *ast.AggregateFuncExpr:
		return "Aggregate", true
	case *ast.HavingClause:
		return "Having", true
	}
	return "Other", false
}

func (v *typeAnalysis) Enter(in ast.Node) (ast.Node, bool) {
	if func_type, ok := v.analyzeTypes(in); ok {
		v.typesList= append(v.typesList, func_type)
	}
	return in, false
}

func (v *typeAnalysis) Leave(in ast.Node) (ast.Node, bool) {
	return in, true
}

// wrap typeAnalysis
func typeVisitor(rootNode ast.StmtNode) []string {
	v := &typeAnalysis{}
	rootNode.Accept(v)
	return v.typesList
}

func parse(sql string) (ast.StmtNode, error) {
	p := parser.New()

	stmtNodes, _, err := p.Parse(sql, "", "")
	if err != nil {
		return nil, err
	}

	fmt.Println(stmtNodes[0])
	return stmtNodes[0], nil
}

func main() {
	if len(os.Args) != 2 {
		fmt.Println("usage: typeAnalysis 'SQL statement'")
		return
	}
	sql := os.Args[1]

	astNode, err := parse(sql)
	if err != nil {
		fmt.Printf("parse error: %v\n", err.Error())
		return
	}

	tpyeList := typeVisitor(astNode)
	fmt.Printf("tpyeList = ", tpyeList)

	fmt.Println("\n Column Parse, Done!")
}
