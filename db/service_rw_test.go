package db

import (
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/Financial-Times/generic-rw-aurora/config"
	"github.com/satori/go.uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

const (
	testTableName      = "draft_annotations"
	testKeyColumn      = "uuid"
	testDocColumn      = "body"
	lastModifiedColumn = "last_modified"
	publishRefColumn   = "publish_ref"

	testDocTemplate = `{"foo":"%s"}`
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
	testDoc := fmt.Sprintf(testDocTemplate, time.Now().String())
	params := map[string]string{"id": testKey}
	metadata := make(map[string]string)
	metadata["timestamp"] = time.Now().UTC().Format("2006-01-02T15:04:05.000Z")
	metadata["publishRef"] = "tid_testread"
	created, err := s.service.Write(testTableName, testKey, testDoc, params, metadata)
	require.NoError(s.T(), err)
	require.True(s.T(), created, "document should have been created")

	actual, err := s.service.Read(testTableName, testKey)
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), testDoc, actual, "document read from store")
}

func (s *ServiceRWTestSuite) TestReadNotFound() {
	testKey := uuid.NewV4().String()

	_, err := s.service.Read(testTableName, testKey)
	assert.EqualError(s.T(), err, sql.ErrNoRows.Error())
}

func (s *ServiceRWTestSuite) TestWriteCreate() {
	testKey := uuid.NewV4().String()
	testDoc := fmt.Sprintf(testDocTemplate, time.Now().String())
	testLastModified := time.Now().UTC().Format("2006-01-02T15:04:05.000Z")
	testPublishRef := "tid_testcreate"

	params := map[string]string{"id": testKey}
	metadata := make(map[string]string)
	metadata["timestamp"] = testLastModified
	metadata["publishRef"] = testPublishRef

	created, err := s.service.Write(testTableName, testKey, testDoc, params, metadata)
	assert.NoError(s.T(), err)
	assert.True(s.T(), created, "document should have been created")

	query := fmt.Sprintf("SELECT %s, %s, %s FROM %s WHERE %s = ?", testDocColumn, lastModifiedColumn, publishRefColumn, testTableName, testKeyColumn)
	row := s.dbConn.QueryRow(query, testKey)
	var actualDoc string
	var actualLastModified string
	var actualPublishRef string
	row.Scan(&actualDoc, &actualLastModified, &actualPublishRef)

	assert.Equal(s.T(), testDoc, actualDoc, "document")
	assert.Equal(s.T(), testLastModified, actualLastModified, "lastModified")
	assert.Equal(s.T(), testPublishRef, actualPublishRef, "publishRef")
}

func (s *ServiceRWTestSuite) TestWriteUpdate() {
	testKey := uuid.NewV4().String()
	testCreateLastModified := time.Now().Truncate(time.Hour).UTC().Format("2006-01-02T15:04:05.000Z")
	testDoc := fmt.Sprintf(testDocTemplate, testCreateLastModified)
	testCreatePublishRef := "tid_testupdate_1"

	params := map[string]string{"id": testKey}
	metadata := make(map[string]string)
	metadata["timestamp"] = testCreateLastModified
	metadata["publishRef"] = testCreatePublishRef

	_, err := s.service.Write(testTableName, testKey, testDoc, params, metadata)
	assert.NoError(s.T(), err)

	testUpdateLastModified := time.Now().UTC().Format("2006-01-02T15:04:05.000Z")
	testDoc = fmt.Sprintf(testDocTemplate, testUpdateLastModified)
	testUpdatePublishRef := "tid_testupdate_2"
	metadata = make(map[string]string)
	metadata["timestamp"] = testUpdateLastModified
	metadata["publishRef"] = testUpdatePublishRef

	updated, err := s.service.Write(testTableName, testKey, testDoc, params, metadata)
	assert.NoError(s.T(), err)
	assert.False(s.T(), updated, "document should have been updated")

	query := fmt.Sprintf("SELECT %s, %s, %s FROM %s WHERE %s = ?", testDocColumn, lastModifiedColumn, publishRefColumn, testTableName, testKeyColumn)
	row := s.dbConn.QueryRow(query, testKey)
	var actualDoc string
	var actualLastModified string
	var actualPublishRef string
	row.Scan(&actualDoc, &actualLastModified, &actualPublishRef)

	assert.Equal(s.T(), testDoc, actualDoc, "document")
	assert.Equal(s.T(), testUpdateLastModified, actualLastModified, "lastModified")
	assert.Equal(s.T(), testUpdatePublishRef, actualPublishRef, "publishRef")
}
