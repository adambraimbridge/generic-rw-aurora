package db

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/Financial-Times/generic-rw-aurora/config"
	"github.com/oliveagle/jsonpath"
	log "github.com/sirupsen/logrus"
)

const hashColumn = "hash"
const conflictLogMessage = "conflict detected in writing document"

const created = true
const updated = false

var errDataNotAffectedByOperation = errors.New("data is not affected by the operation")

type RWMonitor interface {
	Ping() (string, error)
	SchemaCheck() (string, error)
}

type RWService interface {
	Read(table string, key string) (Document, error)
	Write(table string, key string, doc Document, params map[string]string, previousDocumentHash string) (bool, string, error)
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
	if err := service.conn.Ping(); err != nil {
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

func (service *AuroraRWService) Read(tableName string, key string) (Document, error) {
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
		log.WithField("table", tableName).Error("document column is not configured")
		return Document{}, fmt.Errorf("document column is not configured for table %s", tableName)
	}

	query := fmt.Sprintf("SELECT %s, %s FROM %s WHERE %s = ?", docColumn, hashColumn, table.name, table.primaryKey)

	row := service.conn.QueryRow(query, key)

	var docBody string
	var docHash string
	err := row.Scan(&docBody, &docHash)

	if err != nil {
		if err != sql.ErrNoRows {
			log.WithError(err).Error("unable to read from database")
		}
		return Document{}, err
	}

	doc := Document{
		Body: []byte(docBody),
		hash: docHash,
	}
	return doc, nil
}

func (service *AuroraRWService) Write(tableName string, key string, doc Document, params map[string]string, previousDocHash string) (bool, string, error) {
	table := service.rwConfig[tableName]
	doc.hash = hash(doc.Body)
	var status bool
	var err error
	if table.hasConflictDetection {
		if previousDocHash == "" {
			status, err = service.insertDocumentWithConflictDetection(table, key, doc, params)
		} else {
			status, err = service.updateDocumentWithConflictDetection(table, key, doc, params, previousDocHash)
		}
	} else {
		status, err = service.insertDocumentOnDuplicateKeyUpdate(table, key, doc, params)
	}
	return status, doc.hash, err
}

func (service *AuroraRWService) insertDocumentWithConflictDetection(t table, key string, doc Document, params map[string]string) (bool, error) {
	writeLog := log.WithFields(log.Fields{"table": t.name, "key": key})
	columns, bindings := buildInsertComponents(t, key, doc, params)
	insert := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", t.name, columns, strings.Repeat(",?", len(bindings))[1:])

	err := service.executeStatement(insert, bindings)
	if err != nil {
		if strings.HasPrefix(err.Error(), "Error 1062:") {
			writeLog.WithError(err).Error(conflictLogMessage)
			return service.insertDocumentOnDuplicateKeyUpdate(t, key, doc, params)
		}
		writeLog.WithError(err).Error("unable to write to database")
	}
	return created, err
}

func (service *AuroraRWService) updateDocumentWithConflictDetection(t table, key string, doc Document, params map[string]string, previousDocHash string) (bool, error) {
	writeLog := log.WithFields(log.Fields{"table": t.name, "key": key})

	setValues, setBindings := buildUpdateSetComponents(t, key, doc, params)
	bindings := append(setBindings, key, previousDocHash)
	updateStmt := fmt.Sprintf("UPDATE %s SET %s WHERE %s = ? AND %s = ?", t.name, setValues, t.primaryKey, hashColumn)
	err := service.executeStatement(updateStmt, bindings)
	if err != nil {
		if err == errDataNotAffectedByOperation {
			writeLog.WithError(err).Error(conflictLogMessage)
			return service.insertDocumentOnDuplicateKeyUpdate(t, key, doc, params)
		}
		writeLog.WithError(err).Error("unable to write to database")
	}
	return updated, err
}

func (service *AuroraRWService) insertDocumentOnDuplicateKeyUpdate(t table, key string, doc Document, params map[string]string) (bool, error) {
	//writeLog := log.WithFields(log.Fields{"table": t.name, "key": key})
	//columns, bindings := buildInsertComponents(t, key, doc, params)
	return false, nil
}

//insert := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", tableName, insertCols[1:], strings.Repeat(",?", len(values))[1:])
//upsert := insert + " ON DUPLICATE KEY UPDATE "
//// generate a list of columns and bind values for the UPSERT clause
//for col, val := range values {
//	if col != table.primaryKey {
//		upsert += fmt.Sprintf(" %s = ?,", col)
//		bindings = append(bindings, val)
//	}
//}
//upsert = upsert[:len(upsert) - 1]

//func (service *AuroraRWService) Write(tableName string, key string, doc string, previousDocHash string, params map[string]string, metadata map[string]string) (bool,error) {
//	wlog := log.WithFields(log.Fields{"table": tableName, "key": key})
//	table := service.rwConfig[tableName]
//
//	values := service.generateColumnValues(table, key, doc, hash, params, metadata)
//	query := ""
//	bindings := []interface{}{}
//
//	if hash == "" {
//		insertCols := ""
//		for col, val := range values {
//			insertCols += "," + col
//			bindings = append(bindings, val)
//		}
//
//		query = fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", tableName, insertCols[1:], strings.Repeat(",?", len(values))[1:])
//	} else {
//		setValues := ""
//		for col, val := range values {
//			if col != table.primaryKey {
//				setValues += fmt.Sprintf(" %s = ?,", col)
//				bindings = append(bindings, val)
//			}
//		}
//		setValues = setValues[:len(setValues) - 1]
//
//		query = fmt.Sprintf("UPDATE %s SET %s WHERE %s = '%s' AND %s = '%s'", tableName, setValues, "hash", hash, "uuid", key)
//	}
//
//	var created bool
//	res, err := service.conn.Exec(query, bindings...)
//	if err != nil {
//		wlog.WithError(err).Error("unable to write to database")
//	} else {
//		i, _ := res.RowsAffected()
//		created = i == 1
//	}
//	return created, err
//}

func buildInsertComponents(t table, key string, doc Document, params map[string]string) (string, []interface{}) {
	values := generateColumnValues(t, key, doc, params)
	insertCols := ""
	var bindings []interface{}
	for col, val := range values {
		insertCols += "," + col
		bindings = append(bindings, val)
	}
	return insertCols[1:], bindings
}

func buildUpdateSetComponents(t table, key string, doc Document, params map[string]string) (string, []interface{}) {
	values := generateColumnValues(t, key, doc, params)
	setValues := ""
	var bindings []interface{}
	for col, val := range values {
		setValues += "," + col + "=?"
		bindings = append(bindings, val)
	}
	return setValues[1:], bindings
}

func generateColumnValues(table table, key string, doc Document, params map[string]string) map[string]interface{} {
	writeLog := log.WithFields(log.Fields{"table": table.name, "key": key})

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

	values[hashColumn] = doc.hash

	return values
}

func (service *AuroraRWService) executeStatement(stmt string, bindings []interface{}) error {
	fmt.Println(stmt)
	fmt.Println(bindings)
	res, err := service.conn.Exec(stmt, bindings...)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 1 {
		return errDataNotAffectedByOperation
	}
	return nil
}
