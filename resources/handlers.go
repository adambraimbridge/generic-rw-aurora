package resources

import (
	"context"
	"database/sql"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	"github.com/Financial-Times/generic-rw-aurora/db"
	tidutils "github.com/Financial-Times/transactionid-utils-go"
	"github.com/husobee/vestigo"
	log "github.com/sirupsen/logrus"
)

const (
	errNotFound = "No document found."

	documentHashHeader         = "Document-Hash"
	previousDocumentHashHeader = "Previous-Document-Hash"
)

func Read(service db.RWService, table string, timeout time.Duration) http.HandlerFunc {
	return func(writer http.ResponseWriter, request *http.Request) {
		txid := tidutils.GetTransactionIDFromRequest(request)

		ctx, cancelFunc := context.WithTimeout(tidutils.TransactionAwareContext(context.Background(), txid), timeout)
		defer cancelFunc()

		responseCh := make(chan db.Document)
		errorCh := make(chan error)
		id := vestigo.Param(request, "id")

		go func(responseCh chan db.Document, errorCh chan error) {
			doc, err := service.Read(ctx, table, id)

			if err != nil {
				errorCh <- err
				return
			}

			responseCh <- doc

		}(responseCh, errorCh)

		writer.Header().Set("Content-Type", "application/json")

		readLog := log.WithFields(log.Fields{tidutils.TransactionIDKey: txid, "key": id, "table": table})

		select {
		case <-ctx.Done():
			readLog.Error("Document read request timed out")
			writer.WriteHeader(http.StatusGatewayTimeout)
			json.NewEncoder(writer).Encode(map[string]string{"message": "document read request timed out"})

		case doc := <-responseCh:
			readLog.Info("Document found, responding ...")
			writer.Header().Set(documentHashHeader, doc.Hash)
			for k, v := range doc.Metadata {
				writer.Header().Set(k, v)
			}
			writer.Write(doc.Body)

		case err := <-errorCh:
			body := map[string]string{}
			if err == sql.ErrNoRows {
				readLog.Info("Document is missing")
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

func Write(service db.RWService, table string, timeout time.Duration) http.HandlerFunc {
	return func(writer http.ResponseWriter, request *http.Request) {

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

		// start the endpoint timer after we consume the http body
		// being fair to slow writers (ex: slow/bad network connection over vpn).
		txid := tidutils.GetTransactionIDFromRequest(request)
		ctx, cancelFunc := context.WithTimeout(tidutils.TransactionAwareContext(context.Background(), txid), timeout)
		defer cancelFunc()

		responseCh := make(chan statusHashTuple)
		errorCh := make(chan error)

		go func(responseCh chan statusHashTuple, errorCh chan error) {
			doc := db.NewDocument(docBody)
			for k := range request.Header {
				v := request.Header.Get(k)
				doc.Metadata.Set(strings.ToLower(k), v)
			}

			doc.Metadata.Set("_timestamp", time.Now().UTC().Format("2006-01-02T15:04:05.000Z"))

			previousDocHash := request.Header.Get(previousDocumentHashHeader)

			status, hash, err := service.Write(ctx, table, id, doc, params, previousDocHash)

			if err != nil {
				errorCh <- err
				return
			}
			responseCh <- statusHashTuple{status, hash}
		}(responseCh, errorCh)

		writeLog := log.WithFields(log.Fields{tidutils.TransactionIDKey: txid, "key": id, "table": table})

		select {
		case <-ctx.Done():
			writeLog.Error("Document write request timed out")
			writer.WriteHeader(http.StatusGatewayTimeout)
			json.NewEncoder(writer).Encode(map[string]string{"message": "document write request timed out"})

		case err := <-errorCh:
			writer.WriteHeader(http.StatusInternalServerError)
			body := map[string]string{"message": err.Error()}
			json.NewEncoder(writer).Encode(body)

		case statusHashTuple := <-responseCh:
			writer.Header().Set(documentHashHeader, statusHashTuple.hash)
			if statusHashTuple.status == db.Created {
				writer.WriteHeader(http.StatusCreated)
				writeLog.Info("Document has been created")
			} else {
				writer.WriteHeader(http.StatusOK)
				writeLog.Info("Document has been updated")
			}
		}
	}
}

type statusHashTuple struct {
	status bool
	hash   string
}
