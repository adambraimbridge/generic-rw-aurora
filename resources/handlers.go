package resources

import (
	"database/sql"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	"context"

	"github.com/Financial-Times/generic-rw-aurora/db"
	tidutils "github.com/Financial-Times/transactionid-utils-go"
	"github.com/husobee/vestigo"
)

const (
	errNotFound = "No document found."

	documentHashHeader         = "Document-Hash"
	previousDocumentHashHeader = "Previous-Document-Hash"
)

var (
	passthroughHeaders = []string{
		strings.ToLower(tidutils.TransactionIDHeader),
		"x-origin-system-id",
	}
)

func Read(service db.RWService, table string) http.HandlerFunc {
	return func(writer http.ResponseWriter, request *http.Request) {
		txid := tidutils.GetTransactionIDFromRequest(request)
		ctx := tidutils.TransactionAwareContext(context.Background(), txid)
		id := vestigo.Param(request, "id")
		doc, err := service.Read(ctx, table, id)
		writer.Header().Set("Content-Type", "application/json")
		if err == nil {
			writer.Header().Set(documentHashHeader, doc.Hash)
			writer.Write(doc.Body)
		} else {
			body := map[string]string{}
			if err == sql.ErrNoRows {
				writer.WriteHeader(http.StatusNotFound)
				body["message"] = errNotFound
			} else {
				writer.WriteHeader(http.StatusInternalServerError)
				body["message"] = err.Error()
			}
			json.NewEncoder(writer).Encode(body)
		}
	}
}

func Write(service db.RWService, table string) http.HandlerFunc {
	return func(writer http.ResponseWriter, request *http.Request) {
		txid := tidutils.GetTransactionIDFromRequest(request)
		ctx := tidutils.TransactionAwareContext(context.Background(), txid)
		params := make(map[string]string)
		for _, p := range vestigo.ParamNames(request) {
			params[p[1:]] = vestigo.Param(request, p[1:])
		}
		id := vestigo.Param(request, "id")

		writer.Header().Set("Content-Type", "application/json")

		docBody, err := ioutil.ReadAll(request.Body)
		if err != nil {
			writer.WriteHeader(http.StatusInternalServerError)
			body := map[string]string{"message": err.Error()}
			json.NewEncoder(writer).Encode(body)
			return
		}

		doc := db.NewDocument(docBody)
		for _, k := range passthroughHeaders {
			if v := request.Header.Get(k); len(v) > 0 {
				doc.Metadata.Set(k, v)
			}
		}
		doc.Metadata.Set("timestamp", time.Now().UTC().Format("2006-01-02T15:04:05.000Z"))
		doc.Metadata.Set("publishRef", tidutils.GetTransactionIDFromRequest(request))

		previousDocHash := request.Header.Get(previousDocumentHashHeader)

		status, hash, err := service.Write(ctx, table, id, doc, params, previousDocHash)

		if err == nil {
			writer.Header().Set(documentHashHeader, hash)
			if status == db.Created {
				writer.WriteHeader(http.StatusCreated)
			} else {
				writer.WriteHeader(http.StatusOK)
			}
		} else {
			writer.WriteHeader(http.StatusInternalServerError)
			body := map[string]string{"message": err.Error()}
			json.NewEncoder(writer).Encode(body)
		}
	}
}
