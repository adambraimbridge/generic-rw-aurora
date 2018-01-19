package resources

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Financial-Times/generic-rw-aurora/db"
	tidutils "github.com/Financial-Times/transactionid-utils-go"
	"github.com/husobee/vestigo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

const (
	testTable   = "test_table"
	testKey     = "1234"
	docBody     = `{"foo":"bar"}`
	docHash     = "34563ba43d923189d9e3aefd038683ac4f1f1eab72c2684926220d08"
	prevDocHash = "bfd86d638f3ffda37b45ddf35fb29ee387f3bb8df5278db4b40e9e72"
	testSystemId = "test-system-id"
	testTxId    = "tid_test123"
)

type mockRW struct {
	mock.Mock
}

func (m *mockRW) Read(ctx context.Context, table string, key string) (db.Document, error) {
	args := m.Called(ctx, table, key)
	return args.Get(0).(db.Document), args.Error(1)
}

func (m *mockRW) Write(ctx context.Context, table string, key string, doc db.Document, params map[string]string, previousDocumentHash string) (bool, string, error) {
	args := m.Called(ctx, table, key, doc, params, previousDocumentHash)
	return args.Bool(0), args.String(1), args.Error(2)
}

type mockReader struct {
	mock.Mock
}

func (m *mockReader) Read(p []byte) (n int, err error) {
	args := m.Called(p)
	return args.Int(0), args.Error(1)
}

func TestRead(t *testing.T) {
	doc := db.NewDocument([]byte(docBody))
	doc.Hash = docHash

	rw := &mockRW{}
	rw.On("Read", mock.AnythingOfType("*context.valueCtx"), testTable, testKey).Return(doc, nil)

	router := vestigo.NewRouter()
	router.Get(fmt.Sprintf("/%s/:id", testTable), Read(rw, testTable))

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", fmt.Sprintf("/%s/%s", testTable, testKey), nil)

	router.ServeHTTP(w, req)
	actual := w.Result()

	assert.Equal(t, http.StatusOK, actual.StatusCode, "HTTP status")
	assert.Equal(t, "application/json", actual.Header.Get("Content-Type"), "content type")
	body, _ := ioutil.ReadAll(actual.Body)
	assert.Equal(t, docBody, string(body), "response body")
	assert.Equal(t, docHash, actual.Header.Get(documentHashHeader))

	rw.AssertExpectations(t)
}

func TestReadNotFound(t *testing.T) {
	rw := &mockRW{}

	rw.On("Read", mock.AnythingOfType("*context.valueCtx"), testTable, testKey).Return(db.Document{}, sql.ErrNoRows)

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
	assert.Empty(t, actual.Header.Get(documentHashHeader))

	rw.AssertExpectations(t)
}

func TestReadError(t *testing.T) {
	rw := &mockRW{}
	msg := "Some unexpected error"
	rw.On("Read", mock.AnythingOfType("*context.valueCtx"), testTable, testKey).Return(db.Document{}, errors.New(msg))

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
	assert.Empty(t, actual.Header.Get(documentHashHeader))

	rw.AssertExpectations(t)
}

func matchDocument(expectedBody string, expectedMetadataValues map[string]string, expectedMetadataKeys map[string]struct{}) func(db.Document) bool {
	return func(doc db.Document) bool {
		if string(doc.Body) != expectedBody {
			return false
		}

		for k, v := range doc.Metadata {
			if _, found := expectedMetadataKeys[k]; found {
				continue
			}
			expected, found := expectedMetadataValues[k]
			if !found || expected != v {
				return false
			}
		}

		return true
	}
}

func TestWriteCreate(t *testing.T) {
	docMatcher := mock.MatchedBy(matchDocument(docBody,
		map[string]string{
			"publishRef": testTxId,
			strings.ToLower(tidutils.TransactionIDHeader): testTxId,
			"x-origin-system-id": testSystemId,
		},
		map[string]struct{}{
			"timestamp": struct{}{},
		},
	))

	rw := &mockRW{}
	rw.On("Write", mock.AnythingOfType("*context.valueCtx"), testTable, testKey, docMatcher, map[string]string{"id": testKey}, prevDocHash).Return(true, docHash, nil)

	router := vestigo.NewRouter()
	router.Put(fmt.Sprintf("/%s/:id", testTable), Write(rw, testTable))

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("PUT", fmt.Sprintf("/%s/%s", testTable, testKey), strings.NewReader(docBody))
	req.Header.Set(previousDocumentHashHeader, prevDocHash)
	req.Header.Set(tidutils.TransactionIDHeader, testTxId)
	req.Header.Set("X-Origin-System-Id", testSystemId)

	router.ServeHTTP(w, req)
	actual := w.Result()

	assert.Equal(t, http.StatusCreated, actual.StatusCode, "HTTP status")
	assert.Equal(t, docHash, actual.Header.Get(documentHashHeader))

	rw.AssertExpectations(t)
}

func TestWriteUpdate(t *testing.T) {
	rw := &mockRW{}
	rw.On("Write", mock.AnythingOfType("*context.valueCtx"), testTable, testKey, mock.AnythingOfType("db.Document"), map[string]string{"id": testKey}, prevDocHash).Return(false, docHash, nil)

	router := vestigo.NewRouter()
	router.Put(fmt.Sprintf("/%s/:id", testTable), Write(rw, testTable))

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("PUT", fmt.Sprintf("/%s/%s", testTable, testKey), strings.NewReader(docBody))
	req.Header.Set(previousDocumentHashHeader, prevDocHash)

	router.ServeHTTP(w, req)
	actual := w.Result()

	assert.Equal(t, http.StatusOK, actual.StatusCode, "HTTP status")
	assert.Equal(t, docHash, actual.Header.Get(documentHashHeader))

	rw.AssertExpectations(t)
}

func TestWriteError(t *testing.T) {
	rw := &mockRW{}
	msg := "Some unexpected error"
	rw.On("Write", mock.AnythingOfType("*context.valueCtx"), testTable, testKey, mock.AnythingOfType("db.Document"), map[string]string{"id": testKey}, prevDocHash).Return(false, "", errors.New(msg))

	router := vestigo.NewRouter()
	router.Put(fmt.Sprintf("/%s/:id", testTable), Write(rw, testTable))

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("PUT", fmt.Sprintf("/%s/%s", testTable, testKey), strings.NewReader(docBody))
	req.Header.Set(previousDocumentHashHeader, prevDocHash)

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
	doc := db.NewDocument([]byte(docBody))
	doc.Hash = docHash

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
	assert.Empty(t, actual.Header.Get(documentHashHeader))

	rw.AssertExpectations(t)
}
