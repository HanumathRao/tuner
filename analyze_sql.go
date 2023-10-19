package main

import (
	"os"
	"fmt"
	"encoding/json"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	_ "github.com/pingcap/tidb/parser/test_driver"
	_ "github.com/pingcap/tidb/types/parser_driver"
)
import "C"

type typeAnalysis struct {
	typesList []string
}

func (v *typeAnalysis) analyzeTypes(node ast.Node) (string, bool)  {
	switch parse_node := node.(type) {
	case *ast.BetweenExpr, *ast.BinaryOperationExpr:
		return "Filter", true
	case *ast.OrderByClause:
		return "OrderBy", true
	case *ast.GroupByClause:
		return "GroupBy", true
	case *ast.Join:
		if (parse_node.Right != nil) {
			return "Join", true
		}
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

	return stmtNodes[0], nil
}


func analyze_internal(sql string) string {
	astNode, err := parse(sql)
	if err != nil {
		fmt.Printf("parse error: %v\n", err.Error())
		return ""
	}

	typeList := typeVisitor(astNode)
	
	var m = make(map[string]bool)
	var a = []string{}

	for _, value := range typeList { 
		if !m[value] {
			a = append(a, value)
			m[value] = true
		}
    	}
	var tagsJson, _ = json.Marshal(a)
        return string(tagsJson)
}

//export analyze
func analyze(sql *C.char) *C.char {
	return C.CString(analyze_internal(C.GoString(sql)))
}

func hello() {
	fmt.Println("Hello world!")
}

func main() {
	if len(os.Args) != 2 {
		fmt.Println("usage: typeAnalysis 'SQL statement'")
		return
	}
	sql := os.Args[1]

	analyze_internal(sql)
}
