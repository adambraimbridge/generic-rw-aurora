package db

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/Financial-Times/generic-rw-aurora/config"
	tid "github.com/Financial-Times/transactionid-utils-go"
	"github.com/satori/go.uuid"
	//log "github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus"
	logTest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

const (
	testTable                      = "published_annotations"
	testTableWithConflictDetection = "draft_annotations"
	testKeyColumn                  = "uuid"
	testDocColumn                  = "body"
	lastModifiedColumn             = "last_modified"
	publishRefColumn               = "publish_ref"
	testDocTemplate                = `{"foo":"%s"}`
)

type ServiceRWTestSuite struct {
	suite.Suite
	dbAdminUrl string
	dbConn     *sql.DB
	service    *AuroraRWService
}

func TestServiceRWTestSuite(t *testing.T) {
	testSuite := ServiceRWTestSuite{}
	testSuite.dbAdminUrl = getDatabaseURL(t)

	suite.Run(t, &testSuite)
}

func (s *ServiceRWTestSuite) SetupSuite() {
	conn, err := sql.Open("mysql", s.dbAdminUrl)
	require.NoError(s.T(), err)
	defer conn.Close()

	pacSchema := "pac_test"
	pacUser := "pac_test_user"

	err = cleanDatabase(conn, pacUser, pacSchema)
	require.NoError(s.T(), err)

	pacPassword := uuid.NewV4().String()
	err = createDatabase(conn, pacUser, pacPassword, pacSchema)
	require.NoError(s.T(), err)

	i := strings.Index(s.dbAdminUrl, "@")
	j := strings.Index(s.dbAdminUrl, "/")
	dbUrl := fmt.Sprintf("%s:%s@%s/%s", pacUser, pacPassword, s.dbAdminUrl[i+1:j], pacSchema)

	conn, err = Connect(dbUrl)
	require.NoError(s.T(), err)

	cfg, err := config.ReadConfig("../config.yml")
	require.NoError(s.T(), err)

	s.dbConn = conn
	s.service = NewService(conn, true, cfg)
}

func (s *ServiceRWTestSuite) TearDownSuite() {
	s.dbConn.Close()
}

func (s *ServiceRWTestSuite) TestRead() {
	testKey := uuid.NewV4().String()

	testTID := "tid_testread"

	testDocBody := fmt.Sprintf(testDocTemplate, time.Now().String())
	testDoc := NewDocument([]byte(testDocBody))
	testDoc.Metadata.Set("timestamp", time.Now().UTC().Format("2006-01-02T15:04:05.000Z"))
	testDoc.Metadata.Set("publishRef", testTID)

	testCtx := tid.TransactionAwareContext(context.Background(), testTID)

	params := map[string]string{"id": testKey}

	status, expectedDocHash, err := s.service.Write(context.Background(), testTable, testKey, testDoc, params, "")
	require.NoError(s.T(), err)
	require.Equal(s.T(), Created, status)

	actual, err := s.service.Read(testCtx, testTable, testKey)
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), testDoc.Body, actual.Body, "document read from store")
	assert.Equal(s.T(), expectedDocHash, actual.Hash)
}

func (s *ServiceRWTestSuite) TestReadNotFound() {
	testKey := uuid.NewV4().String()
	testTID := "tid_testread"
	testCtx := tid.TransactionAwareContext(context.Background(), testTID)
	_, err := s.service.Read(testCtx, testTable, testKey)
	assert.EqualError(s.T(), err, sql.ErrNoRows.Error())
}

func (s *ServiceRWTestSuite) TestWriteCreateWithoutConflictDetection() {
	testKey := uuid.NewV4().String()
	testLastModified := time.Now().UTC().Format("2006-01-02T15:04:05.000Z")
	testTID := "tid_testcreate"

	testDocBody := fmt.Sprintf(testDocTemplate, time.Now().String())
	testDoc := NewDocument([]byte(testDocBody))
	testDoc.Metadata.Set("timestamp", testLastModified)
	testDoc.Metadata.Set("publishRef", testTID)

	params := map[string]string{"id": testKey}

	testCtx := tid.TransactionAwareContext(context.Background(), testTID)

	status, docHash, err := s.service.Write(testCtx, testTable, testKey, testDoc, params, "")
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), Created, status)

	query := fmt.Sprintf("SELECT %s, %s, %s, %s FROM %s WHERE %s = ?", testDocColumn, lastModifiedColumn, publishRefColumn, hashColumn, testTable, testKeyColumn)
	row := s.dbConn.QueryRow(query, testKey)
	var actualDocBody string
	var actualLastModified string
	var actualPublishRef string
	var actualHash string
	row.Scan(&actualDocBody, &actualLastModified, &actualPublishRef, &actualHash)

	assert.Equal(s.T(), testDocBody, actualDocBody, "document body")
	assert.Equal(s.T(), testLastModified, actualLastModified, "lastModified")
	assert.Equal(s.T(), testTID, actualPublishRef, "publishRef")
	assert.Equal(s.T(), docHash, actualHash, "document hash")
}

func (s *ServiceRWTestSuite) TestWriteUpdateWithoutConflictDetection() {
	testKey := uuid.NewV4().String()
	testCreateLastModified := time.Now().Truncate(time.Hour).UTC().Format("2006-01-02T15:04:05.000Z")
	testDocBody := fmt.Sprintf(testDocTemplate, testCreateLastModified)
	testCreatePublishRef := "tid_testupdate_1"

	testCtx := tid.TransactionAwareContext(context.Background(), testCreatePublishRef)

	testDoc := NewDocument([]byte(testDocBody))
	testDoc.Metadata.Set("timestamp", testCreateLastModified)
	testDoc.Metadata.Set("publishRef", testCreatePublishRef)

	params := map[string]string{"id": testKey}

	_, _, err := s.service.Write(testCtx, testTable, testKey, testDoc, params, "")
	require.NoError(s.T(), err)

	testUpdateLastModified := time.Now().UTC().Format("2006-01-02T15:04:05.000Z")
	testDocBody = fmt.Sprintf(testDocTemplate, testUpdateLastModified)
	testUpdatePublishRef := "tid_testupdate_2"

	testDoc = NewDocument([]byte(testDocBody))
	testDoc.Metadata.Set("timestamp", testUpdateLastModified)
	testDoc.Metadata.Set("publishRef", testUpdatePublishRef)

	testCtx = tid.TransactionAwareContext(context.Background(), testCreatePublishRef)

	status, docHash, err := s.service.Write(testCtx, testTable, testKey, testDoc, params, "")
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), Updated, status)

	query := fmt.Sprintf("SELECT %s, %s, %s, %s FROM %s WHERE %s = ?", testDocColumn, lastModifiedColumn, publishRefColumn, hashColumn, testTable, testKeyColumn)
	row := s.dbConn.QueryRow(query, testKey)
	var actualDocBody string
	var actualLastModified string
	var actualPublishRef string
	var actualHash string
	row.Scan(&actualDocBody, &actualLastModified, &actualPublishRef, &actualHash)

	assert.Equal(s.T(), testDocBody, actualDocBody, "document body")
	assert.Equal(s.T(), testUpdateLastModified, actualLastModified, "lastModified")
	assert.Equal(s.T(), testUpdatePublishRef, actualPublishRef, "publishRef")
	assert.Equal(s.T(), docHash, actualHash, "document hash")
}

func (s *ServiceRWTestSuite) TestWriteCreateWithoutConflict() {
	hook := logTest.NewGlobal()
	testKey := uuid.NewV4().String()
	testLastModified := time.Now().UTC().Format("2006-01-02T15:04:05.000Z")
	testTID := "tid_testcreate"

	testDocBody := fmt.Sprintf(testDocTemplate, time.Now().String())
	testDoc := NewDocument([]byte(testDocBody))
	testDoc.Metadata.Set("timestamp", testLastModified)
	testDoc.Metadata.Set("publishRef", testTID)

	params := map[string]string{"id": testKey}

	testCtx := tid.TransactionAwareContext(context.Background(), testTID)

	status, docHash, err := s.service.Write(testCtx, testTableWithConflictDetection, testKey, testDoc, params, "")
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), Created, status)

	query := fmt.Sprintf("SELECT %s, %s, %s, %s FROM %s WHERE %s = ?", testDocColumn, lastModifiedColumn, publishRefColumn, hashColumn, testTableWithConflictDetection, testKeyColumn)
	row := s.dbConn.QueryRow(query, testKey)
	var actualDocBody string
	var actualLastModified string
	var actualPublishRef string
	var actualHash string
	row.Scan(&actualDocBody, &actualLastModified, &actualPublishRef, &actualHash)

	assert.Equal(s.T(), testDocBody, actualDocBody, "document body")
	assert.Equal(s.T(), testLastModified, actualLastModified, "lastModified")
	assert.Equal(s.T(), testTID, actualPublishRef, "publishRef")
	assert.Equal(s.T(), docHash, actualHash, "document hash")

	assert.Empty(s.T(), hook.AllEntries())
}

func (s *ServiceRWTestSuite) TestWriteCreateWithConflict() {
	hook := logTest.NewGlobal()
	testKey := uuid.NewV4().String()
	testLastModified := time.Now().UTC().Format("2006-01-02T15:04:05.000Z")
	testTID1 := "tid_testcreate_1"
	testTID2 := "tid_testcreate_2"

	testDocBody := fmt.Sprintf(testDocTemplate, time.Now().String())
	testDoc := NewDocument([]byte(testDocBody))
	testDoc.Metadata.Set("timestamp", testLastModified)
	testDoc.Metadata.Set("publishRef", testTID1)

	params := map[string]string{"id": testKey}

	testCtx := tid.TransactionAwareContext(context.Background(), testTID1)

	status, docHash, err := s.service.Write(testCtx, testTableWithConflictDetection, testKey, testDoc, params, "")
	require.NoError(s.T(), err)
	require.Equal(s.T(), Created, status)

	testLastModified2 := time.Now().UTC().Format("2006-01-02T15:04:05.000Z")
	testDocBody = fmt.Sprintf(testDocTemplate, testLastModified)

	testDoc = NewDocument([]byte(testDocBody))
	testDoc.Metadata.Set("timestamp", testLastModified2)
	testDoc.Metadata.Set("publishRef", testTID2)

	testCtx = tid.TransactionAwareContext(context.Background(), testTID2)

	status, docHash, err = s.service.Write(testCtx, testTableWithConflictDetection, testKey, testDoc, params, "")
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), Updated, status)

	query := fmt.Sprintf("SELECT %s, %s, %s, %s FROM %s WHERE %s = ?", testDocColumn, lastModifiedColumn, publishRefColumn, hashColumn, testTableWithConflictDetection, testKeyColumn)
	row := s.dbConn.QueryRow(query, testKey)
	var actualDocBody string
	var actualLastModified string
	var actualPublishRef string
	var actualHash string
	row.Scan(&actualDocBody, &actualLastModified, &actualPublishRef, &actualHash)

	assert.Equal(s.T(), testDocBody, actualDocBody, "document body")
	assert.Equal(s.T(), testLastModified2, actualLastModified, "lastModified")
	assert.Equal(s.T(), testTID2, actualPublishRef, "publishRef")
	assert.Equal(s.T(), docHash, actualHash, "document hash")

	assert.Equal(s.T(), "conflict detected while updating document", hook.LastEntry().Message)
	assert.Equal(s.T(), logrus.ErrorLevel, hook.LastEntry().Level)
	assert.Equal(s.T(), testKey, hook.LastEntry().Data["key"])
	assert.Equal(s.T(), testTableWithConflictDetection, hook.LastEntry().Data["table"])
	assert.Equal(s.T(), testTID2, hook.LastEntry().Data[tid.TransactionIDKey])
}

func (s *ServiceRWTestSuite) TestUpdateWithoutConflict() {
	hook := logTest.NewGlobal()
	testKey := uuid.NewV4().String()

	testTID1 := "tid_testupdate_1"
	testTID2 := "tid_testupdate_2"

	testDocBody := fmt.Sprintf(testDocTemplate, time.Now().String())
	testDoc := NewDocument([]byte(testDocBody))
	testDoc.Metadata.Set("timestamp", time.Now().UTC().Format("2006-01-02T15:04:05.000Z"))
	testDoc.Metadata.Set("publishRef", testTID1)

	params := map[string]string{"id": testKey}

	testCtx := tid.TransactionAwareContext(context.Background(), testTID1)

	status, previousDocHash, err := s.service.Write(testCtx, testTableWithConflictDetection, testKey, testDoc, params, "")
	require.NoError(s.T(), err)
	require.Equal(s.T(), Created, status)

	testLastModified := time.Now().UTC().Format("2006-01-02T15:04:05.000Z")
	testDocBody = fmt.Sprintf(testDocTemplate, time.Now().String())
	testDoc = NewDocument([]byte(testDocBody))
	testDoc.Metadata.Set("timestamp", testLastModified)
	testDoc.Metadata.Set("publishRef", testTID2)

	status, docHash, err := s.service.Write(testCtx, testTableWithConflictDetection, testKey, testDoc, params, previousDocHash)
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), status, Updated)

	query := fmt.Sprintf("SELECT %s, %s, %s, %s FROM %s WHERE %s = ?", testDocColumn, lastModifiedColumn, publishRefColumn, hashColumn, testTableWithConflictDetection, testKeyColumn)
	row := s.dbConn.QueryRow(query, testKey)
	var actualDocBody string
	var actualLastModified string
	var actualPublishRef string
	var actualHash string
	row.Scan(&actualDocBody, &actualLastModified, &actualPublishRef, &actualHash)

	assert.Equal(s.T(), testDocBody, actualDocBody, "document body")
	assert.Equal(s.T(), testLastModified, actualLastModified, "lastModified")
	assert.Equal(s.T(), testTID2, actualPublishRef, "publishRef")
	assert.Equal(s.T(), docHash, actualHash, "document hash")

	assert.Empty(s.T(), hook.AllEntries())
}

func (s *ServiceRWTestSuite) TestUpdateWithConflict() {
	hook := logTest.NewGlobal()
	testKey := uuid.NewV4().String()

	testTID1 := "tid_testupdate_1"
	testTID2 := "tid_testupdate_2"

	testDocBody := fmt.Sprintf(testDocTemplate, time.Now().String())
	testDoc := NewDocument([]byte(testDocBody))
	testDoc.Metadata.Set("timestamp", time.Now().UTC().Format("2006-01-02T15:04:05.000Z"))
	testDoc.Metadata.Set("publishRef", testTID1)

	params := map[string]string{"id": testKey}

	testCtx := tid.TransactionAwareContext(context.Background(), testTID1)

	status, _, err := s.service.Write(testCtx, testTableWithConflictDetection, testKey, testDoc, params, "")
	require.NoError(s.T(), err)
	require.Equal(s.T(), Created, status)

	testLastModified := time.Now().UTC().Format("2006-01-02T15:04:05.000Z")
	testDocBody = fmt.Sprintf(testDocTemplate, time.Now().String())
	testDoc = NewDocument([]byte(testDocBody))
	testDoc.Metadata.Set("timestamp", testLastModified)
	testDoc.Metadata.Set("publishRef", testTID2)

	aVeryOldHash := "01234567890123456789012345678901234567890123456789012345"

	testCtx = tid.TransactionAwareContext(context.Background(), testTID2)

	status, docHash, err := s.service.Write(testCtx, testTableWithConflictDetection, testKey, testDoc, params, aVeryOldHash)
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), status, Updated)

	query := fmt.Sprintf("SELECT %s, %s, %s, %s FROM %s WHERE %s = ?", testDocColumn, lastModifiedColumn, publishRefColumn, hashColumn, testTableWithConflictDetection, testKeyColumn)
	row := s.dbConn.QueryRow(query, testKey)
	var actualDocBody string
	var actualLastModified string
	var actualPublishRef string
	var actualHash string
	row.Scan(&actualDocBody, &actualLastModified, &actualPublishRef, &actualHash)

	assert.Equal(s.T(), testDocBody, actualDocBody, "document body")
	assert.Equal(s.T(), testLastModified, actualLastModified, "lastModified")
	assert.Equal(s.T(), testTID2, actualPublishRef, "publishRef")
	assert.Equal(s.T(), docHash, actualHash, "document hash")

	assert.Equal(s.T(), "conflict detected while updating document", hook.LastEntry().Message)
	assert.Equal(s.T(), logrus.ErrorLevel, hook.LastEntry().Level)
	assert.Equal(s.T(), testKey, hook.LastEntry().Data["key"])
	assert.Equal(s.T(), testTableWithConflictDetection, hook.LastEntry().Data["table"])
	assert.Equal(s.T(), testTID2, hook.LastEntry().Data[tid.TransactionIDKey])
}
