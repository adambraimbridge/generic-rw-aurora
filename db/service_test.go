package db

import (
	"database/sql"
	"fmt"
	"strings"
	"testing"

	"github.com/satori/go.uuid"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type ServiceTestSuite struct {
	suite.Suite
	dbAdminUrl string
	dbUrl string
}

func TestServiceTestSuite(t *testing.T) {
	testSuite := ServiceTestSuite{}
	testSuite.dbAdminUrl = getDatabaseURL(t)
	suite.Run(t, &testSuite)
}

func (s *ServiceTestSuite) SetupSuite() {
	conn, err := sql.Open("mysql", s.dbAdminUrl)
	require.NoError(s.T(), err)
	defer conn.Close()

	pacSchema := "pac_test";
	pacUser := "pac_test_user";

	userExists, err := checkUserExists(conn, pacUser)
	require.NoError(s.T(), err)
	schemaExists, err := checkSchemaExists(conn, pacSchema)
	require.NoError(s.T(), err)

	if userExists {
		log.Infof("dropping user %s", pacUser)
		conn.Exec(fmt.Sprintf(`DROP USER %s`, pacUser))
	}

	if schemaExists {
		log.Infof("dropping schema %s", pacSchema)
		conn.Exec(fmt.Sprintf(`DROP DATABASE %s`, pacSchema))
	}

	_, err = conn.Exec(fmt.Sprintf(`CREATE DATABASE %s`, pacSchema))
	require.NoError(s.T(), err)

	pacPassword := uuid.NewV4().String();
	_, err = conn.Exec(fmt.Sprintf(`CREATE USER %s IDENTIFIED BY '%s'`, pacUser, pacPassword))
	require.NoError(s.T(), err)

	_, err = conn.Exec(fmt.Sprintf(`GRANT ALL ON %s.* TO %s`, pacSchema, pacUser))
	require.NoError(s.T(), err)

	i := strings.Index(s.dbAdminUrl, "@")
	j := strings.Index(s.dbAdminUrl, "/")
	s.dbUrl = fmt.Sprintf("%s:%s@%s/%s", pacUser, pacPassword, s.dbAdminUrl[i+1:j], pacSchema)
}

func checkUserExists(conn *sql.DB, user string) (bool, error){
	var count int
	rows, err := conn.Query("SELECT COUNT(*) FROM information_schema.user_privileges WHERE grantee LIKE ?", fmt.Sprintf(`'%s'%%`, user))
	if err != nil {
		return false, err
	}

	defer rows.Close()
	rows.Next()
	rows.Scan(&count)

	return count > 0, nil
}

func checkSchemaExists(conn *sql.DB, schema string) (bool, error){
	var count int
	rows, err := conn.Query("SELECT COUNT(*) FROM information_schema.schema_privileges WHERE table_schema = ?", schema)
	if err != nil {
		return false, err
	}

	defer rows.Close()
	rows.Next()
	rows.Scan(&count)

	return count > 0, nil
}

func (s *ServiceTestSuite) TestPing() {
	conn, _ := Connect(s.dbUrl)
	defer conn.Close()

	srv := NewService(conn, false)

	msg, err := srv.Ping()
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), "Ping OK", msg)
}

func (s *ServiceTestSuite) TestSchemaMigrate() {
	conn, _ := Connect(s.dbUrl)
	defer conn.Close()

	// first, test without migrating and check for version mismatch
	srv := NewService(conn, false)

	msg, err := srv.SchemaCheck()
	assert.EqualError(s.T(), err, fmt.Sprintf("migrating database from 0 to %d is required", requiredVersion))
	assert.Equal(s.T(), "Database schema is mismatched to this service", msg)

	// now, reconnect and migrate
	srv = NewService(conn, true)

	msg, err = srv.SchemaCheck()
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), fmt.Sprintf("Database schema is at version %d", requiredVersion), msg)
}
