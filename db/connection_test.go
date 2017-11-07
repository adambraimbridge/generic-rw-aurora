package db

import (
	"os"
	"strings"
	"testing"
	"github.com/stretchr/testify/assert"
)

func getDatabaseURL(t *testing.T) string {
	if testing.Short() {
		t.Skip("Database integration for long tests only.")
	}

	dbURL := os.Getenv("DB_TEST_URL")
	if strings.TrimSpace(dbURL) == "" {
		t.Fatal("Please set the environment variable DB_TEST_URL to run database integration tests (e.g. export DB_TEST_URL=user:pass@host:port/dbName). Alternatively, run `go test -short` to skip them.")
	}

	return dbURL
}

func TestConnect(t *testing.T) {
	dbUrl := getDatabaseURL(t)

	conn, err := Connect(dbUrl)

	assert.NoError(t, err, "unable to connect to test database")
	assert.NotNil(t, conn,"returned database connection")
	conn.Close()
}

func TestConnectError(t *testing.T) {
	conn, err := Connect("foo:bar@nowhere.example.com/nodatabase")

	assert.Error(t, err, "unable to connect to test database")
	assert.NotNil(t, conn,"returned database connection")
	conn.Close()
}
