package db

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/Financial-Times/generic-rw-aurora/config"
	"github.com/oliveagle/jsonpath"
	log "github.com/sirupsen/logrus"
)

const (
	testSql = "SELECT COUNT(*) FROM goose_db_version"
)

type RWMonitor interface {
	Ping() (string, error)
	SchemaCheck() (string, error)
}

type RWService interface {
	Read(table string, key string) (string, error)
	Write(table string, key string, doc string, params map[string]string, metadata map[string]string) (bool,error)
}

type table struct {
	name       string
	columns    map[string]string
	primaryKey string
}

type AuroraRWService struct {
	conn           *sql.DB
	schemaVersion  int64
	schemaMismatch error
	rwConfig       map[string]table
}

func (t *table) columnMapping() string {
	var mapping string
	for col, expr := range t.columns {
		mapping += fmt.Sprintf(",%s->%s", expr, col)
	}

	return mapping[1:]
}

func NewService(conn *sql.DB, migrate bool, rwConfig *config.Config) *AuroraRWService {
	tables := make(map[string]table)
	for _, tableConfig := range rwConfig.Paths {
		t := table{tableConfig.Table, tableConfig.Columns, tableConfig.PrimaryKey}
		tables[tableConfig.Table] = t
		log.WithFields(log.Fields{"table": t.name, "primaryKey": t.primaryKey, "columnMapping": t.columnMapping()}).Info("mapping initialised")
	}
	service := &AuroraRWService{conn: conn, rwConfig: tables}

	if err := service.migrate(migrate); err != nil {
		log.WithError(err).Error("failed to migrate db")
		service.schemaMismatch = err
	}

	return service
}

func (service *AuroraRWService) Ping() (string, error) {
	var result interface{}
	if err := service.conn.QueryRow(testSql).Scan(&result); err != nil {
		return fmt.Sprintf("Ping Not OK: %s", err.Error()), err
	}

	return "Ping OK", nil
}

func (service *AuroraRWService) SchemaCheck() (string, error) {
	if service.schemaMismatch == nil {
		return fmt.Sprintf("Database schema is at version %d", service.schemaVersion), nil
	}

	return "Database schema is mismatched to this service", service.schemaMismatch
}

func (service *AuroraRWService) Read(tableName string, key string) (string, error) {
	log.WithField("table", tableName).WithField("key", key).Info("Read")
	table := service.rwConfig[tableName]
	var docColumn string
	for col, expr := range table.columns {
		if expr == "$" {
			docColumn = col
			break
		}
	}
	if docColumn == "" {
		log.WithField("table", tableName).Error("no document column is configured")
		return "", fmt.Errorf("no document column is configured for table %s", tableName)
	}

	query := fmt.Sprintf("SELECT %s FROM %s WHERE %s = ?", docColumn, table.name, table.primaryKey)

	row := service.conn.QueryRow(query, key)

	var doc string
	err := row.Scan(&doc)

	if err != nil {
		if err != sql.ErrNoRows {
			log.WithError(err).Error("unable to read from database")
		}
		return "", err
	}

	return doc, nil
}

func (service *AuroraRWService) Write(tableName string, key string, doc string, params map[string]string, metadata map[string]string) (bool,error) {
	wlog := log.WithFields(log.Fields{"table": tableName, "key": key})
	table := service.rwConfig[tableName]

	values := service.generateColumnValues(table, key, doc, params, metadata)

	// generate a list of columns and bind values for the INSERT clause
	insertCols := ""
	bindings := []interface{}{}
	for col, val := range values {
		insertCols += "," + col
		bindings = append(bindings, val)
	}

	insert := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", tableName, insertCols[1:], strings.Repeat(",?", len(values))[1:])
	upsert := insert + " ON DUPLICATE KEY UPDATE "
	// generate a list of columns and bind values for the UPSERT clause
	for col, val := range values {
		if col != table.primaryKey {
			upsert += fmt.Sprintf(" %s = ?,", col)
			bindings = append(bindings, val)
		}
	}
	upsert = upsert[:len(upsert) - 1]

	var created bool
	res, err := service.conn.Exec(upsert, bindings...)
	if err != nil {
		wlog.WithError(err).Error("unable to write to database")
	} else {
		i, _ := res.RowsAffected()
		created = i == 1
	}
	return created, err
}

func (service *AuroraRWService) generateColumnValues(table table, key string, doc string, params map[string]string, metadata map[string]string) map[string]interface{} {
	wlog := log.WithFields(log.Fields{"table": table.name, "key": key})

	values := make(map[string]interface{})
	var jsondoc interface{}

	for col, expr := range table.columns {
		var val interface{}
		if strings.HasPrefix(expr, ":") {
			// : - in the request params, e.g. :id
			val = params[expr[1:]]
		} else if strings.HasPrefix(expr, "@.") {
			// @. - in the metadata, e.g. @.timestamp
			val = metadata[expr[2:]]
		} else if expr == "$" {
			// $ - the whole document
			val = doc
		} else if strings.HasPrefix(expr, "$") {
			// $. - a JSONpath in the document, e.g. $.post.body
			// only unmarshal into a JSON document if necessary, and only once
			if jsondoc == nil {
				json.Unmarshal([]byte(doc), &jsondoc)
			}
			var err error
			val, err = jsonpath.JsonPathLookup(jsondoc, expr)
			if err != nil {
				wlog.WithFields(log.Fields{"column": col, "expr": expr}).Warn("unable to extract JSONPath value from document")
			}
		} else {
			// a literal value
			val = expr
		}
		values[col] = val
	}

	return values
}
