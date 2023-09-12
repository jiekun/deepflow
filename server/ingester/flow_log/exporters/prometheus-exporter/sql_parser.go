package prometheus_exporter

import (
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	_ "github.com/pingcap/tidb/types/parser_driver"
	"regexp"
)

var (
	shardingRegex *regexp.Regexp
)

func GetStmtTypeAndTableName(sql string) (string, string) {
	command, tableName := "unknown", "unknown"

	p := parser.New()
	stmt, err := p.ParseOneStmt(sql, "", "")
	if err != nil {
		return command, tableName
	}

	var from *ast.TableRefsClause
	switch st := stmt.(type) {
	case *ast.SelectStmt:
		from = st.From
		command = "SELECT"
	case *ast.InsertStmt:
		from = st.Table
		command = "INSERT"
	case *ast.UpdateStmt:
		from = st.TableRefs
		command = "UPDATE"
	case *ast.DeleteStmt:
		from = st.TableRefs
		command = "DELETE"
	}
	if from != nil && from.TableRefs != nil {
		ts, ok := from.TableRefs.Left.(*ast.TableSource)
		if ok {
			tn, ok := ts.Source.(*ast.TableName)
			if ok {
				tableName = tn.Name.String()
			}
		}
	}
	return command, removeShardingInfo(tableName)
}

func removeShardingInfo(tableName string) string {
	return shardingRegex.ReplaceAllString(tableName, "")
}
