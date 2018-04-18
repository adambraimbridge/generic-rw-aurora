package db

import (
	"math"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
	maxConnections := 5
	conn, err := Connect(dbUrl, maxConnections)

	require.NotNil(t, conn, "returned database connection")
	defer conn.Close()

	assert.NoError(t, err, "unable to connect to test database")

	wg := sync.WaitGroup{}
	var actualOpenConnections int64
	for i := 1; i < 100 * maxConnections; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rows, err := conn.Query("SELECT 1")
			assert.NoError(t, err)

			if err == nil {
				defer rows.Close()

				assert.True(t, rows.Next(), "should have returned a row")

				var actual int
				rows.Scan(&actual)
				assert.Equal(t, 1, actual, "read from database")

				previousMaxOpenConnections := atomic.LoadInt64(&actualOpenConnections)
				openConnections := math.Max(float64(previousMaxOpenConnections), float64(conn.Stats().OpenConnections))
				atomic.StoreInt64(&actualOpenConnections, int64(openConnections))
			}
		}()
	}
	wg.Wait()
	assert.True(t, int(actualOpenConnections) <= maxConnections, "maximum open connections (actual: %v, expected notGreaterThan: %v)", actualOpenConnections, maxConnections)
}

func TestConnectError(t *testing.T) {
	conn, err := Connect("foo:bar@nowhere.example.com/nodatabase", 5)

	assert.Error(t, err, "unable to connect to test database")
	assert.NotNil(t, conn, "returned database connection")
	conn.Close()
}
