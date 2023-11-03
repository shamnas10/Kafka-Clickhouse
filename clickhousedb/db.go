package clickhousedb

import (
	"fmt"

	"database/sql"

	_ "github.com/ClickHouse/clickhouse-go"
)

func DB() (*sql.DB, error) {

	var db *sql.DB

	var err error

	connStr := "tcp://localhost:9000?username=default&password=root&database=mydb"

	db, err = sql.Open("clickhouse", connStr)

	if err != nil {

		fmt.Printf("Error opening clickhouse connection: %v", err)

	}

	return db, nil

}
