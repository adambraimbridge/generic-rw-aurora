package resources

import (
	"database/sql"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/Financial-Times/generic-rw-aurora/db"
	tidutils "github.com/Financial-Times/transactionid-utils-go"
	"github.com/husobee/vestigo"
)

const (
	errNotFound = "No document found."
)

func Read(service db.RWService, table string) http.HandlerFunc {
	return func(writer http.ResponseWriter, request *http.Request) {
		id := vestigo.Param(request, "id")
		doc, hash, err := service.Read(table, id)
		writer.Header().Set("Content-Type", "application/json")
		if err == nil {
			writer.Header().Set("X-Hash-Header", hash)
			writer.Write([]byte(doc))
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
		metadata := make(map[string]string)
		metadata["timestamp"] = time.Now().UTC().Format("2006-01-02T15:04:05.000Z")
		metadata["publishRef"] = tidutils.GetTransactionIDFromRequest(request)

		params := make(map[string]string)
		for _, p := range vestigo.ParamNames(request) {
			params[p[1:]] = vestigo.Param(request, p[1:])
		}
		id := vestigo.Param(request, "id")

		writer.Header().Set("Content-Type", "application/json")

		doc, err := ioutil.ReadAll(request.Body)
		if err != nil {
			writer.WriteHeader(http.StatusInternalServerError)
			body := map[string]string{"message": err.Error()}
			json.NewEncoder(writer).Encode(body)
			return
		}

		hash := request.Header.Get("X-Hash-Header")
		created, err := service.Write(table, id, string(doc), hash, params, metadata)
		if err == nil {
			if created {
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
