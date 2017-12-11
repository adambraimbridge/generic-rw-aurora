package db

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/Financial-Times/generic-rw-aurora/config"
	tid "github.com/Financial-Times/transactionid-utils-go"
	"github.com/oliveagle/jsonpath"
	log "github.com/sirupsen/logrus"
)

const (
	testSql = "SELECT COUNT(*) FROM goose_db_version"
)

const hashColumn = "hash"
const conflictLogMessage = "conflict detected while updating document"

const Created = true
const Updated = false

const contextDocumentKey = "contextDocumentKey"
const contextTable = "contextTable"

var errDataNotAffectedByOperation = errors.New("data is not affected by the operation")

type RWMonitor interface {
	Ping() (string, error)
	SchemaCheck() (string, error)
}

type RWService interface {
	Read(ctx context.Context, table string, key string) (Document, error)
	Write(ctx context.Context, table string, key string, doc Document, params map[string]string, previousDocumentHash string) (bool, string, error)
}

type table struct {
	name                 string
	columns              map[string]string
	primaryKey           string
	hasConflictDetection bool
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
		t := table{
			tableConfig.Table,
			tableConfig.Columns,
			tableConfig.PrimaryKey,
			tableConfig.HasConflictDetection,
		}
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

func (service *AuroraRWService) Read(ctx context.Context, tableName string, key string) (Document, error) {
	txid, _ := tid.GetTransactionIDFromContext(ctx)
	readLog := log.WithField("table", tableName).
		WithField("key", key).
		WithField(tid.TransactionIDKey, txid)

	readLog.Info("Reading document from database")
	table := service.rwConfig[tableName]
	var docColumn string

	for col, expr := range table.columns {
		if expr == "$" {
			docColumn = col
			break
		}
	}

	if docColumn == "" {
		readLog.Error("document column is not configured")
		return Document{}, fmt.Errorf("document column is not configured for table %s", tableName)
	}

	query := fmt.Sprintf("SELECT %s, %s FROM %s WHERE %s = ?", docColumn, hashColumn, table.name, table.primaryKey)

	row := service.conn.QueryRow(query, key)

	var docBody string
	var docHash string
	err := row.Scan(&docBody, &docHash)

	if err != nil {
		if err != sql.ErrNoRows {
			readLog.WithError(err).Error("unable to read from database")
		}
		return Document{}, err
	}

	doc := Document{
		Body: []byte(docBody),
		Hash: docHash,
	}
	return doc, nil
}

func (service *AuroraRWService) Write(ctx context.Context, tableName string, key string, doc Document, params map[string]string, previousDocHash string) (bool, string, error) {
	ctx = context.WithValue(ctx, contextTable, tableName)
	ctx = context.WithValue(ctx, contextDocumentKey, key)
	table := service.rwConfig[tableName]
	doc.Hash = hash(doc.Body)
	var status bool
	var err error
	if table.hasConflictDetection {
		if previousDocHash == "" {
			status, err = service.insertDocumentWithConflictDetection(ctx, table, key, doc, params)
		} else {
			status, err = service.updateDocumentWithConflictDetection(ctx, table, key, doc, params, previousDocHash)
		}
	} else {
		status, err = service.insertDocumentOnDuplicateKeyUpdate(ctx, table, key, doc, params)
	}
	return status, doc.Hash, err
}

func (service *AuroraRWService) insertDocumentWithConflictDetection(ctx context.Context, t table, key string, doc Document, params map[string]string) (bool, error) {
	writeLog := buildLogEntryFromContext(ctx)
	columns, values, bindings := buildInsertComponents(ctx, t, key, doc, params)
	insert := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", t.name, columns, values)

	_, err := service.executeStatement(insert, bindings)
	if err != nil {
		if strings.HasPrefix(err.Error(), "Error 1062:") {
			writeLog.WithError(err).Error(conflictLogMessage)
			return service.insertDocumentOnDuplicateKeyUpdate(ctx, t, key, doc, params)
		}
		writeLog.WithError(err).Error("unable to write to database")
	}
	return Created, err
}

func (service *AuroraRWService) updateDocumentWithConflictDetection(ctx context.Context, t table, key string, doc Document, params map[string]string, previousDocHash string) (bool, error) {
	writeLog := buildLogEntryFromContext(ctx)

	setValues, setBindings := buildUpdateSetComponents(ctx, t, key, doc, params)
	bindings := append(setBindings, key, previousDocHash)
	updateStmt := fmt.Sprintf("UPDATE %s SET %s WHERE %s = ? AND %s = ?", t.name, setValues, t.primaryKey, hashColumn)
	affectedRows, err := service.executeStatement(updateStmt, bindings)
	if err != nil {
		writeLog.WithError(err).Error("unable to write to database")
	}
	if affectedRows == 0 {
		writeLog.WithError(errDataNotAffectedByOperation).Error(conflictLogMessage)
		return service.insertDocumentOnDuplicateKeyUpdate(ctx, t, key, doc, params)
	}
	return Updated, err
}

func (service *AuroraRWService) insertDocumentOnDuplicateKeyUpdate(ctx context.Context, t table, key string, doc Document, params map[string]string) (bool, error) {
	writeLog := buildLogEntryFromContext(ctx)
	columns, values, insertBindings := buildInsertComponents(ctx, t, key, doc, params)
	setValues, setBindings := buildUpdateSetComponents(ctx, t, key, doc, params)
	bindings := append(insertBindings, setBindings...)

	insertStmt := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", t.name, columns, values)
	insertStmt += " ON DUPLICATE KEY UPDATE " + setValues
	affectedRows, err := service.executeStatement(insertStmt, bindings)
	if err != nil {
		writeLog.WithError(err).Error("Error in writing ")
	}
	if affectedRows == 1 {
		return Created, err
	}
	return Updated, err
}

func buildInsertComponents(ctx context.Context, t table, key string, doc Document, params map[string]string) (string, string, []interface{}) {
	valuesMap := generateColumnValuesMap(ctx, t, key, doc, params)
	insertCols := ""
	valuesStmt := ""
	var bindings []interface{}
	for col, val := range valuesMap {
		insertCols += "," + col
		valuesStmt += ",?"
		bindings = append(bindings, val)
	}
	return insertCols[1:], valuesStmt[1:], bindings
}

func buildUpdateSetComponents(ctx context.Context, t table, key string, doc Document, params map[string]string) (string, []interface{}) {
	valuesMap := generateColumnValuesMap(ctx, t, key, doc, params)
	setValues := ""
	var bindings []interface{}
	for col, val := range valuesMap {
		setValues += "," + col + "=?"
		bindings = append(bindings, val)
	}
	return setValues[1:], bindings
}

func generateColumnValuesMap(ctx context.Context, table table, key string, doc Document, params map[string]string) map[string]interface{} {
	writeLog := buildLogEntryFromContext(ctx)

	values := make(map[string]interface{})
	var jsondoc interface{}

	for col, expr := range table.columns {
		var val interface{}
		if strings.HasPrefix(expr, ":") {
			// : - in the request params, e.g. :id
			val = params[expr[1:]]
		} else if strings.HasPrefix(expr, "@.") {
			// @. - in the metadata, e.g. @.timestamp
			val = doc.Metadata[expr[2:]]
		} else if expr == "$" {
			// $ - the whole document
			val = doc.Body
		} else if strings.HasPrefix(expr, "$") {
			// $. - a JSONpath in the document, e.g. $.post.body
			// only unmarshal into a JSON document if necessary, and only once
			if jsondoc == nil {
				json.Unmarshal(doc.Body, &jsondoc)
			}
			var err error
			val, err = jsonpath.JsonPathLookup(jsondoc, expr)
			if err != nil {
				writeLog.WithFields(log.Fields{"column": col, "expr": expr}).Warn("unable to extract JSONPath value from document")
			}
		} else {
			// a literal value
			val = expr
		}
		values[col] = val
	}

	values[hashColumn] = doc.Hash

	return values
}

func (service *AuroraRWService) executeStatement(stmt string, bindings []interface{}) (int64, error) {
	res, err := service.conn.Exec(stmt, bindings...)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return n, nil
}

func buildLogEntryFromContext(ctx context.Context) *log.Entry {
	txid := ctx.Value(tid.TransactionIDKey)
	key := ctx.Value(contextDocumentKey)
	table := ctx.Value(contextTable)
	return log.WithFields(log.Fields{"table": table, "key": key, tid.TransactionIDKey: txid})
}
