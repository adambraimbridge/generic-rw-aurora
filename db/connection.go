package db

import (
	"database/sql"

	_ "github.com/go-sql-driver/mysql"
)

func Connect(dbUrl string) (*sql.DB,error) {
	db, err := sql.Open("mysql", dbUrl)

	if err == nil {
		err = db.Ping() // force a meaningful connection check
		// we may return a *sql.DB even when there seems to be a connection error - it might recover
	}

	return db, err
}
