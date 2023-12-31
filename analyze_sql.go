package main

import (
	"os"
	"fmt"
	"encoding/json"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	_ "github.com/pingcap/tidb/parser/test_driver"
	_ "github.com/pingcap/tidb/pkg/types/parser_driver"
)
import "C"

// column analysis visitor
type columnAnalysis struct {
	columnCount uint64
}

func (v *columnAnalysis) analyzeColumns(node ast.Node) (uint64, bool) {
	switch node.(type) {
	case *ast.ColumnName, *ast.ColumnDef:
		return 1, true
	// Temporary fix since visitor is not working for extract. TODO: fix visitor.
	case *ast.TimeUnitExpr:
		return 1, true
	}
	return 0, true
}

func (v *columnAnalysis) Enter(in ast.Node) (ast.Node, bool) {
	if column_count, ok := v.analyzeColumns(in); ok {
		v.columnCount = v.columnCount+column_count
	}
	return in, false
}

func (v *columnAnalysis) Leave(in ast.Node) (ast.Node, bool) {
	return in, true
}

func columnVisitor(rootNode ast.Node) uint64 {
	v := &columnAnalysis{}
	rootNode.Accept(v)
	return v.columnCount
}

// end of column analysis visitor
type typeAnalysis struct {
	typesList []string
}

func (v *typeAnalysis) analyzeTypes(node ast.Node) ([]string, bool)  {
	switch nodeType := node.(type) {
	case *ast.ExplainStmt, *ast.ExplainForStmt:
		return []string {"Explain"}, true
	case *ast.ShowStmt, *ast.SetStmt, *ast.UseStmt,
		*ast.BeginStmt, *ast.CommitStmt, *ast.SavepointStmt, *ast.ReleaseSavepointStmt,
		*ast.RollbackStmt, *ast.CreateUserStmt, *ast.SetPwdStmt:
		return []string {"System"}, true
	case *ast.AnalyzeTableStmt:
		return []string {"Analyze"}, true
	case *ast.DeleteStmt:
		return []string {"Delete"}, true
	case *ast.UpdateStmt:
		return []string {"Update"}, true
	case *ast.InsertStmt:
		return []string {"Insert"}, true
	case *ast.SubqueryExpr, *ast.ExistsSubqueryExpr:
		return []string {"InnerJoin"}, true
	case *ast.PatternInExpr:
		if (nodeType.Sel == nil) {
			return []string {"Filter"}, true
		}
	case *ast.AggregateFuncExpr:
		if (nodeType.Distinct) {
			return []string {"Aggregate","Distinct"}, true
		}
		return []string {"Aggregate"}, true
	case ast.ExprNode:
		columnCount := columnVisitor(node)
		_, valueExpr := node.(ast.ValueExpr)
		if (!valueExpr && (columnCount == 0)) {
			return []string{"Constant"}, true
		}
	case *ast.OrderByClause:
		return []string{"OrderBy"}, true
	case *ast.GroupByClause:
		return []string {"GroupBy"}, true
	case *ast.SelectStmt:
		if (nodeType.AfterSetOperator != nil && (*nodeType.AfterSetOperator == ast.Union || *nodeType.AfterSetOperator == ast.UnionAll)) {
			return []string {"Select","Union"}, true
		}
		markers := []string{"Select"}
		if (nodeType.Where != nil) {
			markers = append(markers, "Filter")
		}
		if (nodeType.Distinct) {
			markers = append(markers, "Distinct")
		}
		if (len(markers) > 0) {
			return markers, true
		}
	case *ast.Join:
		if (nodeType.Right != nil) {
			switch nodeType.Tp {
				case ast.LeftJoin:
					return []string {"LeftJoin"}, true
				case ast.RightJoin:
					return []string {"RightJoin"}, true
				default:
					return []string {"InnerJoin"}, true
			}
		}
	case *ast.HavingClause:
		return []string {"Having"}, true
	}
	return []string {"Other"}, false
}

func (v *typeAnalysis) Enter(in ast.Node) (ast.Node, bool) {
	if func_type, ok := v.analyzeTypes(in); ok {
		v.typesList= append(v.typesList, func_type...)
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
        // Not sure why we end up here with no error and no ast
	if len(stmtNodes) == 0 {
		return nil, nil
	} else {
		return stmtNodes[0], nil
	}
}

func analyze_internal(sql string, fulllist bool) (string) {
	astNode, err := parse(sql)
	if (err != nil  || astNode == nil) {
		return "[]"
	}

	typeList := typeVisitor(astNode)
	var m = make(map[string]bool)
	var a = []string{}

	for _, value := range typeList { 
		if (!m[value] || fulllist) {
			a = append(a, value)
			m[value] = true
		}
    	}
	var tagsJson, _ = json.Marshal(a)
        return string(tagsJson)
}

//export analyze
func analyze(sql *C.char, fulllist bool) (*C.char ) {
	keys := analyze_internal(C.GoString(sql), fulllist)
	return C.CString(keys) 
}

func main() {
	if len(os.Args) != 2 {
		fmt.Println("usage: typeAnalysis 'SQL statement'")
		return
	}
	sql := os.Args[1]

	result := analyze_internal(sql, true)
        print("\n result = ", result)
}
