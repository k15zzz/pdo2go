package pdo2go

import (
	"database/sql"
	"log"
)

type IPDOStatement interface {
	Execute(args Arguments) bool
	BindValue(param string, value string)
	Fetch() (Row, bool)
	FetchAll() (Rows, bool)
	RowCount() (int, error)
	ColumnCount() (int, error)
}

type PDOStatement struct {
	sql         string
	pdo         PDO
	arg         Arguments
	rowSql      *sql.Rows
	statusFetch bool
}

func NewPDOStatement(sql string, pdo PDO) PDOStatement {
	return PDOStatement{pdo: pdo, sql: sql}
}

// Execute executes an SQL statement in a single
// function call, returning the number of rows affected by the statement.
func (pdoStmt *PDOStatement) Execute(args Arguments) bool {
	pdoStmt.sql = buildQuery(pdoStmt.sql, args)

	if pdoStmt.arg != nil {
		pdoStmt.sql = buildQuery(pdoStmt.sql, pdoStmt.arg)
	}

	return true
}

// Exec executes an SQL statement in a single function call,
// returning the number of rows affected by the statement
func (pdoStmt *PDOStatement) Exec() int64 {
	if pdoStmt.arg != nil {
		pdoStmt.sql = buildQuery(pdoStmt.sql, pdoStmt.arg)
	}

	return pdoStmt.pdo.Exec(pdoStmt.sql)
}

// FetchAll returns an array containing all of the remaining rows in the result set.
// The array represents each row as either an array of column values or an object
// with properties corresponding to each column name
func (pdoStmt *PDOStatement) FetchAll() (Rows, bool) {
	if pdoStmt.arg != nil {
		pdoStmt.sql = buildQuery(pdoStmt.sql, pdoStmt.arg)
	}

	return pdoStmt.pdo.Query(pdoStmt.sql), true
}

// Fetch a row from a result set associated with a PDOStatement object.
func (pdoStmt *PDOStatement) Fetch() (Row, bool) {
	if pdoStmt.arg != nil {
		pdoStmt.sql = buildQuery(pdoStmt.sql, pdoStmt.arg)
	}

	if !pdoStmt.statusFetch {
		db, err := openSQLDB(pdoStmt.pdo.driverName, pdoStmt.pdo.dataSourceName)
		if err != nil {
			log.Fatal(err)
		}

		rows, err := db.Query(pdoStmt.sql)
		if err != nil {
			log.Fatal(err)
		}

		pdoStmt.statusFetch = true
		pdoStmt.rowSql = rows
	}

	status := pdoStmt.rowSql.Next()

	var row Row
	if status {
		row = getRowMap(pdoStmt.rowSql)
	} else {
		row = nil
	}

	if !status {
		defer pdoStmt.rowSql.Close()
	}

	return row, status
}

// RowCount returns the number of rows resulting from a SELECT operation
func (pdoStmt *PDOStatement) RowCount() (int, error) {
	if pdoStmt.arg != nil {
		pdoStmt.sql = buildQuery(pdoStmt.sql, pdoStmt.arg)
	}

	rows := pdoStmt.pdo.Query(pdoStmt.sql)

	return len(rows), nil
}

// ColumnCount returns the number of column resulting from a SELECT operation
func (pdoStmt *PDOStatement) ColumnCount() (int, error) {
	if pdoStmt.arg != nil {
		pdoStmt.sql = buildQuery(pdoStmt.sql, pdoStmt.arg)
	}

	rows := pdoStmt.pdo.Query(pdoStmt.sql)

	return len(rows[0]), nil
}

// BindValue adding an argument to an argument group one at a time
func (pdoStmt *PDOStatement) BindValue(key string, value string) {
	pdoStmt.arg[key] = value
}
