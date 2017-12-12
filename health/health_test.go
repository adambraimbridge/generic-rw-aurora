package health

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type mockRWMonitor struct {
	mock.Mock
}

func (m *mockRWMonitor) Ping() (string, error) {
	args := m.Called()
	return args.String(0), args.Error(1)
}

func (m *mockRWMonitor) SchemaCheck() (string, error) {
	args := m.Called()
	return args.String(0), args.Error(1)
}

func TestGTG_OK(t *testing.T) {
	rw := &mockRWMonitor{}
	rw.On("Ping").Return("OK", nil)
	h := NewHealthService("test-systemCode", "test-appName", "test-appDescription", rw)

	gtg := h.GTG()
	assert.True(t, gtg.GoodToGo, "GTG")
	assert.Equal(t, "OK", gtg.Message)

	rw.AssertExpectations(t)
}

func TestGTG_NotConnected(t *testing.T) {
	rw := &mockRWMonitor{}
	err := errors.New("test error")
	rw.On("Ping").Return("Not OK", err)
	h := NewHealthService("test-systemCode", "test-appName", "test-appDescription", rw)

	gtg := h.GTG()
	assert.False(t, gtg.GoodToGo, "GTG")
	assert.Equal(t, "Not connected to database", gtg.Message)

	rw.AssertExpectations(t)
}

func TestHealth_OK(t *testing.T) {
	rw := &mockRWMonitor{}
	rw.On("Ping").Return("OK", nil)
	rw.On("SchemaCheck").Return("OK", nil)
	h := NewHealthService("test-systemCode", "test-appName", "test-appDescription", rw)

	for _, c := range h.Checks {
		_, err := c.Checker()
		assert.NoError(t, err)
	}

	rw.AssertExpectations(t)
}

func TestHealth_NotConnected(t *testing.T) {
	rw := &mockRWMonitor{}
	err := errors.New("not connected")
	rw.On("Ping").Return("Not OK", err)
	rw.On("SchemaCheck").Return("Not OK", err)
	h := NewHealthService("test-systemCode", "test-appName", "test-appDescription", rw)

	for _, c := range h.Checks {
		_, actual := c.Checker()
		assert.EqualError(t, actual, err.Error())
	}

	rw.AssertExpectations(t)
}

func TestHealth_SchemaMismatch(t *testing.T) {
	rw := &mockRWMonitor{}
	rw.On("Ping").Return("OK", nil)
	err := errors.New("schema mismatch")
	rw.On("SchemaCheck").Return("Not OK", err)
	h := NewHealthService("test-systemCode", "test-appName", "test-appDescription", rw)

	for _, c := range h.Checks {
		_, actual := c.Checker()
		if actual == nil {
			assert.Equal(t, "check-db-connection", c.ID, "ID of healthy check")
		} else {
			assert.Equal(t, "check-db-schema", c.ID, "ID of unhealthy check")
			assert.EqualError(t, actual, err.Error())
		}
	}

	rw.AssertExpectations(t)
}
