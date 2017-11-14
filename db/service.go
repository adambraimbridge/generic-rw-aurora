package db

import (
	"database/sql"
	"fmt"

	log "github.com/sirupsen/logrus"
)

const (
	testSql = "SELECT COUNT(*) FROM goose_db_version"
)

type AuroraRWService interface {
	Ping() (string, error)
	SchemaCheck() (string, error)
}

type auroraRW struct {
	conn           *sql.DB
	schemaVersion int64
	schemaMismatch error
}

func NewService(conn *sql.DB, migrate bool) AuroraRWService {
	service := &auroraRW{conn: conn}

	if err := service.migrate(migrate); err != nil {
		log.WithError(err).Error("failed to migrate db")
		service.schemaMismatch = err
	}

	return service
}

func (service *auroraRW) Ping() (string, error) {
	var result interface{}
	if err := service.conn.QueryRow(testSql).Scan(&result); err != nil {
		return fmt.Sprintf("Ping Not OK: %s", err.Error()), err
	}

	return "Ping OK", nil
}

func (service *auroraRW) SchemaCheck() (string, error) {
	if service.schemaMismatch == nil {
		return fmt.Sprintf("Database schema is at version %d", service.schemaVersion), nil
	}

	return "Database schema is mismatched to this service", service.schemaMismatch
}
