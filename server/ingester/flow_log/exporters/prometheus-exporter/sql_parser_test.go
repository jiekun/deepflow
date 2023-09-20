package prometheus_exporter

import "testing"

func TestGetStmtTypeAndTableName(t *testing.T) {
	tests := []struct {
		name      string
		sql       string
		tableName string
		operation string
	}{
		{
			"select",
			"select * from my_schema.my_table_select where a = b;",
			"my_table_select",
			"SELECT",
		},
		{
			"insert",
			"insert into my_schema.my_table_insert (a,b,c) values (e,f,g);",
			"my_table_insert",
			"INSERT",
		},
		{
			"update",
			"UPDATE my_schema.my_table_update set a=b limit 100;",
			"my_table_update",
			"UPDATE",
		},
		{
			"delete",
			"delete from my_schema.my_table_delete limit 100;",
			"my_table_delete",
			"DELETE",
		},
		{
			"join",
			"select * from my_table_a left join my_table_b on my_table_a.id = my_table_b.id;",
			"my_table_a",
			"SELECT",
		},
		{
			"sharding table name",
			"select * from sharding_table_01 left join my_table_b on my_table_a.id = my_table_b.id;",
			"sharding_table",
			"SELECT",
		},
		{
			"sharding table name 2",
			"select * from sharding_table_12347982 left join my_table_b on my_table_a.id = my_table_b.id;",
			"sharding_table",
			"SELECT",
		},
		{
			"test complex sql",
			"select a, b from (select a, b, c from my_table_a)",
			"sub_query",
			"SELECT",
		},
		{
			"test meaning less sql",
			"select 1",
			"",
			"SELECT",
		},
		{
			"test meaning less sql 2",
			"begin;",
			"",
			"",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			operation, tableName := GetStmtTypeAndTableName(tt.sql)
			if tableName != tt.tableName {
				t.Errorf("GetStmtTypeAndTableName() got = %v, want %v", tableName, tt.tableName)
			}
			if operation != tt.operation {
				t.Errorf("GetStmtTypeAndTableName() got1 = %v, want %v", operation, tt.operation)
			}
		})
	}
}
