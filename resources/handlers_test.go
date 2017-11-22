package resources

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/husobee/vestigo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

const (
	testTable = "test_table"
	testKey   = "1234"
	doc = `{"foo":"bar"}`
)

type mockRW struct {
	mock.Mock
}

func (m *mockRW) Read(table string, key string) (string, error) {
	args := m.Called(table, key)
	return args.String(0), args.Error(1)
}

func (m *mockRW) Write(table string, key string, doc string, params map[string]string, metadata map[string]string) (bool, error) {
	args := m.Called(table, key, doc, params, metadata)
	return args.Bool(0), args.Error(1)
}

type mockReader struct {
	mock.Mock
}

func (m *mockReader) Read(p []byte) (n int, err error) {
	args := m.Called(p)
	return args.Int(0), args.Error(1)
}

func TestRead(t *testing.T) {
	rw := &mockRW{}
	rw.On("Read", testTable, testKey).Return(doc, nil)

	router := vestigo.NewRouter()
	router.Get(fmt.Sprintf("/%s/:id", testTable), Read(rw, testTable))

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", fmt.Sprintf("/%s/%s", testTable, testKey), nil)

	router.ServeHTTP(w, req)
	actual := w.Result()

	assert.Equal(t, http.StatusOK, actual.StatusCode, "HTTP status")
	assert.Equal(t, "application/json", actual.Header.Get("Content-Type"), "content type")
	body, _ := ioutil.ReadAll(actual.Body)
	assert.Equal(t, doc, string(body), "response body")

	rw.AssertExpectations(t)
}

func TestReadNotFound(t *testing.T) {
	rw := &mockRW{}

	rw.On("Read", testTable, testKey).Return("", sql.ErrNoRows)

	router := vestigo.NewRouter()
	router.Get(fmt.Sprintf("/%s/:id", testTable), Read(rw, testTable))

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", fmt.Sprintf("/%s/%s", testTable, testKey), nil)

	router.ServeHTTP(w, req)
	actual := w.Result()

	assert.Equal(t, http.StatusNotFound, actual.StatusCode, "HTTP status")
	assert.Equal(t, "application/json", actual.Header.Get("Content-Type"), "content type")
	var errorResponse map[string]string
	json.NewDecoder(actual.Body).Decode(&errorResponse)
	assert.Equal(t, "No document found.", errorResponse["message"])

	rw.AssertExpectations(t)
}

func TestReadError(t *testing.T) {
	rw := &mockRW{}
	msg := "Some unexpected error"
	rw.On("Read", testTable, testKey).Return("", errors.New(msg))

	router := vestigo.NewRouter()
	router.Get(fmt.Sprintf("/%s/:id", testTable), Read(rw, testTable))

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", fmt.Sprintf("/%s/%s", testTable, testKey), nil)

	router.ServeHTTP(w, req)
	actual := w.Result()

	assert.Equal(t, http.StatusInternalServerError, actual.StatusCode, "HTTP status")
	assert.Equal(t, "application/json", actual.Header.Get("Content-Type"), "content type")
	var errorResponse map[string]string
	json.NewDecoder(actual.Body).Decode(&errorResponse)
	assert.Equal(t, msg, errorResponse["message"])

	rw.AssertExpectations(t)
}

func TestWriteCreate(t *testing.T) {
	rw := &mockRW{}
	rw.On("Write", testTable, testKey, doc, map[string]string{"id":testKey}, mock.AnythingOfType("map[string]string")).Return(true, nil)

	router := vestigo.NewRouter()
	router.Put(fmt.Sprintf("/%s/:id", testTable), Write(rw, testTable))

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("PUT", fmt.Sprintf("/%s/%s", testTable, testKey), strings.NewReader(doc))

	router.ServeHTTP(w, req)
	actual := w.Result()

	assert.Equal(t, http.StatusCreated, actual.StatusCode, "HTTP status")

	rw.AssertExpectations(t)
}

func TestWriteUpdate(t *testing.T) {
	rw := &mockRW{}
	rw.On("Write", testTable, testKey, doc, map[string]string{"id":testKey}, mock.AnythingOfType("map[string]string")).Return(false, nil)

	router := vestigo.NewRouter()
	router.Put(fmt.Sprintf("/%s/:id", testTable), Write(rw, testTable))

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("PUT", fmt.Sprintf("/%s/%s", testTable, testKey), strings.NewReader(doc))

	router.ServeHTTP(w, req)
	actual := w.Result()

	assert.Equal(t, http.StatusOK, actual.StatusCode, "HTTP status")

	rw.AssertExpectations(t)
}

func TestWriteError(t *testing.T) {
	rw := &mockRW{}
	msg := "Some unexpected error"
	rw.On("Write", testTable, testKey, doc, map[string]string{"id":testKey}, mock.AnythingOfType("map[string]string")).Return(false, errors.New(msg))

	router := vestigo.NewRouter()
	router.Put(fmt.Sprintf("/%s/:id", testTable), Write(rw, testTable))

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("PUT", fmt.Sprintf("/%s/%s", testTable, testKey), strings.NewReader(doc))

	router.ServeHTTP(w, req)
	actual := w.Result()

	assert.Equal(t, http.StatusInternalServerError, actual.StatusCode, "HTTP status")
	assert.Equal(t, "application/json", actual.Header.Get("Content-Type"), "content type")
	var errorResponse map[string]string
	json.NewDecoder(actual.Body).Decode(&errorResponse)
	assert.Equal(t, msg, errorResponse["message"])

	rw.AssertExpectations(t)
}

func TestWriteEntityReadError(t *testing.T) {
	rw := &mockRW{}

	router := vestigo.NewRouter()
	router.Put(fmt.Sprintf("/%s/:id", testTable), Write(rw, testTable))

	msg := "read entity error"
	reader := mockReader{}
	reader.On("Read", mock.Anything).Return(0, errors.New(msg))
	w := httptest.NewRecorder()
	req, _ := http.NewRequest("PUT", fmt.Sprintf("/%s/%s", testTable, testKey), &reader)

	router.ServeHTTP(w, req)
	actual := w.Result()

	assert.Equal(t, http.StatusInternalServerError, actual.StatusCode, "HTTP status")
	assert.Equal(t, "application/json", actual.Header.Get("Content-Type"), "content type")
	var errorResponse map[string]string
	json.NewDecoder(actual.Body).Decode(&errorResponse)
	assert.Equal(t, msg, errorResponse["message"])

	rw.AssertExpectations(t)
}
