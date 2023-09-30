package pdo2go

import (
	"database/sql"
	"fmt"
	"log"
	"strconv"
	"strings"
)

func buildQuery(sql string, args Arguments) string {
	for key, value := range args {
		if _, err := strconv.Atoi(value); err == nil {
			sql = strings.Replace(sql, key, value, -1)
		} else {
			sql = strings.Replace(sql, key, "'"+value+"'", -1)
		}
	}
	fmt.Println("sql:", sql)
	return sql
}

func buildRowsMap(rows *sql.Rows, rslt *Rows) {
	row := getRowMap(rows)
	*rslt = append(*rslt, row)
}

func getRowMap(rows *sql.Rows) Row {
	cols, cols_err := rows.Columns()

	if cols_err != nil {
		log.Fatalln(cols_err)
	}

	rawResult := make([][]byte, len(cols))
	result := make([]string, len(cols))

	dest := make([]interface{}, len(cols))

	for i, _ := range cols {
		dest[i] = &rawResult[i]
	}

	rows.Scan(dest...)

	for i, raw := range rawResult {
		result[i] = string(raw)
	}

	row := Row{}

	for j, v := range result {
		row[cols[j]] = v
	}

	return row
}

func openSQLDB(driverName string, dataSourceName string) (*sql.DB, error) {
	return sql.Open(driverName, dataSourceName)
}
