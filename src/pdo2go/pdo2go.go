package pdo2go

import (
	"context"
	"database/sql"
	"log"
	"reflect"
)

type Arguments map[string]string

type Row map[string]string

type Rows []Row

type IPDO interface {
	Query(statement string) Rows           //
	Prepare(statement string) PDOStatement //
	Exec(statement string) int64           //
	BeginTransaction() bool
	Commit() bool
	RollBack() bool
	InTransaction() bool
}

type PDO struct {
	driverName     string
	dataSourceName string
	tx             *sql.Tx
	ctx            context.Context
}

func NewPDO(driverName string, dataSourceName string) PDO {
	return PDO{driverName: driverName, dataSourceName: dataSourceName}
}

// Prepare a statement for execution and returns a statement object
func (pdo *PDO) Prepare(statement string) PDOStatement {
	if pdo.InTransaction() {
		log.Fatal("Is transaction")
	}

	return NewPDOStatement(statement, *pdo)
}

// Query executes an SQL statement without placeholders
func (pdo *PDO) Query(statement string) Rows {
	db, err := openSQLDB(pdo.driverName, pdo.dataSourceName)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	rows := make(Rows, 0)

	var sqlRows *sql.Rows
	if pdo.InTransaction() {
		sqlRows, err = pdo.tx.QueryContext(pdo.ctx, statement)
	} else {
		sqlRows, err = db.Query(statement)
	}

	if err != nil {
		log.Fatal(err)
	}
	defer sqlRows.Close()

	for sqlRows.Next() {
		buildRowsMap(sqlRows, &rows)
	}

	return rows
}

// Exec - execute an SQL statement and return the number of affected rows
func (pdo *PDO) Exec(statement string) int64 {
	db, err := openSQLDB(pdo.driverName, pdo.dataSourceName)
	defer db.Close()

	var result sql.Result
	if pdo.InTransaction() {
		result, err = pdo.tx.Exec(statement)
	} else {
		result, err = db.Exec(statement)
	}
	if err != nil {
		log.Fatal(err)
	}

	rowsAffected, err := result.RowsAffected()

	if err != nil {
		log.Fatal(err)
	}

	return rowsAffected
}

// BeginTransaction - initiates a transaction
func (pdo *PDO) BeginTransaction() bool {
	if pdo.InTransaction() {
		return true
	}

	db, err := openSQLDB(pdo.driverName, pdo.dataSourceName)

	if err != nil {
		log.Fatal(err)
		return false
	}
	defer db.Close()

	ctx := context.Background()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		log.Fatal(err)
		return false
	}

	pdo.ctx = ctx
	pdo.tx = tx

	return true
}

// Commit - commits a transaction
func (pdo *PDO) Commit() bool {
	if !pdo.InTransaction() {
		log.Fatal("Not transaction")
		return false
	}

	err := pdo.tx.Commit()
	if err != nil {
		log.Fatal(err)
		pdo.RollBack()
		return false
	}

	return true
}

// InTransaction - checks if inside a transaction
func (pdo *PDO) InTransaction() bool {
	return reflect.TypeOf(pdo.ctx) != nil
}

// RollBack - rolls back a transaction
func (pdo *PDO) RollBack() bool {
	if !pdo.InTransaction() {
		log.Fatal("Not transaction")
		return false
	}

	err := pdo.tx.Rollback()

	return err == nil
}
