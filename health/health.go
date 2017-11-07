package health

import (
	"net/http"

	"github.com/Financial-Times/generic-rw-aurora/db"
	fthealth "github.com/Financial-Times/go-fthealth/v1_1"
	"github.com/Financial-Times/service-status-go/gtg"
	log "github.com/sirupsen/logrus"
)

type HealthService struct {
	fthealth.HealthCheck
	db db.AuroraRWService
}

func NewHealthService(appSystemCode string, appName string, appDescription string, rw db.AuroraRWService) *HealthService {
	h := &HealthService{
		fthealth.HealthCheck{
			appSystemCode,
			appName,
			appDescription,
			[]fthealth.Check{},
		},
		rw,
	}
	h.Checks = append(h.Checks, h.dbPingCheck(), h.dbSchemaCheck())

	return h
}

// HealthCheckHandleFunc provides the http endpoint function
func (service *HealthService) HealthCheckHandleFunc() func(w http.ResponseWriter, r *http.Request) {
	return fthealth.Handler(service)
}

// GTG returns the current gtg status
func (service *HealthService) GTG() gtg.Status {
	msg, err := service.dbPingCheck().Checker()
	if err != nil {
		log.WithError(err).Infof("not connected to database: %s", msg)
		return gtg.Status{GoodToGo: false, Message: "Not connected to database"}
	}

	return gtg.Status{GoodToGo: true, Message: "OK"}
}

func (service *HealthService) dbPingCheck() fthealth.Check {
	return fthealth.Check{
		ID:               "check-db-connection",
		BusinessImpact:   "Editorial cannot make changes to annotations for content.",
		Name:             "Check database connection",
		PanicGuide:       "https://dewey.ft.com/generic-rw-aurora.html",
		Severity:         1,
		TechnicalSummary: "Application is not connected to the database.",
		Checker:          service.db.Ping,
	}
}

func (service *HealthService) dbSchemaCheck() fthealth.Check {
	return fthealth.Check{
		ID:               "check-db-schema",
		BusinessImpact:   "Editorial may not be able to make changes to annotations for content.",
		Name:             "Check database schema version",
		PanicGuide:       "https://dewey.ft.com/generic-rw-aurora.html",
		Severity:         1,
		TechnicalSummary: "The database schema is not the version expected by the application.",
		Checker:          service.db.SchemaCheck,
	}
}
