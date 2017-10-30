package db

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPing(t *testing.T) {
	dbUrl := getDatabaseURL(t)
	conn, _ := Connect(dbUrl)

	srv := NewService(conn, false)

	msg, err := srv.Ping()
	assert.NoError(t, err)
	assert.Equal(t, "Ping OK", msg)
}

func TestSchemaMigrate(t *testing.T) {
	dbUrl := getDatabaseURL(t)
	conn, _ := Connect(dbUrl)

	// first, test without migrating and check for version mismatch
	srv := NewService(conn, false)

	msg, err := srv.SchemaCheck()
	assert.EqualError(t, err, fmt.Sprintf("migrating database from 0 to %d is required", requiredVersion))
	assert.Equal(t, "Database schema is mismatched to this service", msg)

	// now, reconnect and migrate
	srv = NewService(conn, true)

	msg, err = srv.SchemaCheck()
	assert.NoError(t, err)
	assert.Equal(t, fmt.Sprintf("Database schema is at version %d", requiredVersion), msg)
}
