package db

import (
	"database/sql"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	log "github.com/sirupsen/logrus"
)

func Connect(dbUrl string, maxConnections int) (*sql.DB, error) {
	i := strings.Index(dbUrl, ":")
	j := strings.Index(dbUrl, "@")
	log.Infof("Connecting to %s:********@%s", dbUrl[:i], dbUrl[j+1:])

	db, err := sql.Open("mysql", dbUrl)

	if err == nil {
		err = db.Ping() // force a meaningful connection check
		// we may return a *sql.DB even when there seems to be a connection error - it might recover
	}

	log.Infof("Maximum DB connections = %v", maxConnections)
	db.SetMaxOpenConns(maxConnections)

	return db, err
}
