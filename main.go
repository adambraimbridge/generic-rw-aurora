package main

import (
	"net/http"
	"os"
	"time"

	api "github.com/Financial-Times/api-endpoint"
	"github.com/Financial-Times/generic-rw-aurora/config"
	"github.com/Financial-Times/generic-rw-aurora/db"
	"github.com/Financial-Times/generic-rw-aurora/health"
	"github.com/Financial-Times/generic-rw-aurora/resources"
	"github.com/Financial-Times/http-handlers-go/httphandlers"
	status "github.com/Financial-Times/service-status-go/httphandlers"
	"github.com/husobee/vestigo"
	"github.com/jawher/mow.cli"
	"github.com/rcrowley/go-metrics"
	log "github.com/sirupsen/logrus"
)

const (
	systemCode     = "generic-rw-aurora"
	appDescription = "Generic R/W for Aurora"
)

func main() {
	app := cli.App(systemCode, appDescription)

	appSystemCode := app.String(cli.StringOpt{
		Name:   "app-system-code",
		Value:  systemCode,
		Desc:   "System Code of the application",
		EnvVar: "APP_SYSTEM_CODE",
	})

	appName := app.String(cli.StringOpt{
		Name:   "app-name",
		Value:  systemCode,
		Desc:   "Application name",
		EnvVar: "APP_NAME",
	})
	appTimeout := app.String(cli.StringOpt{
		Name:   "app-timeout",
		Value:  "8s",
		Desc:   "Application endpoints timeout in milliseconds",
		EnvVar: "APP_TIMEOUT",
	})

	port := app.String(cli.StringOpt{
		Name:   "port",
		Value:  "8080",
		Desc:   "Port to listen on",
		EnvVar: "PORT",
	})

	dbURL := app.String(cli.StringOpt{
		Name:   "db-connection-url",
		Value:  "/pac",
		Desc:   "Database connection URL",
		EnvVar: "DB_CONNECTION_URL",
	})

	performSchemaMigrations := app.Bool(cli.BoolOpt{
		Name:   "db-perform-schema-migrations",
		Value:  false,
		Desc:   "Whether to perform database schema migrations on startup",
		EnvVar: "DB_PERFORM_SCHEMA_MIGRATIONS",
	})

	rwYml := app.String(cli.StringOpt{
		Name:   "rw-config",
		Value:  "./config.yml",
		Desc:   "Location of the RW configuration YML file.",
		EnvVar: "RW_CONFIG",
	})

	apiYml := app.String(cli.StringOpt{
		Name:   "api-yml",
		Value:  "./api.yml",
		Desc:   "Location of the API Swagger YML file.",
		EnvVar: "API_YML",
	})

	log.SetFormatter(&log.JSONFormatter{})
	log.SetLevel(log.InfoLevel)
	log.Infof("[Startup] %v is starting", *appSystemCode)

	app.Action = func() {
		log.Infof("System code: %s, App Name: %s, Port: %s", *appSystemCode, *appName, *port)

		rwConfig, err := config.ReadConfig(*rwYml)
		if err != nil {
			log.WithError(err).Fatal("unable to read r/w YAML configuration")
		}

		conn, err := db.Connect(*dbURL)
		if err != nil {
			log.WithError(err).Error("unable to connect to database")
		}

		rw := db.NewService(conn, *performSchemaMigrations, rwConfig)

		healthService := health.NewHealthService(*appSystemCode, *appName, appDescription, rw)

		timeout, err := time.ParseDuration(*appTimeout)

		if err != nil {
			log.WithError(err).Error("unable to parse timeout")
			return
		}
		serveEndpoints(*port, apiYml, rwConfig, rw, healthService, timeout)
	}

	err := app.Run(os.Args)
	if err != nil {
		log.WithError(err).Error("App could not start")
	}
}

func serveEndpoints(port string, apiYml *string, rw *config.Config, db db.RWService, healthService *health.HealthService, timeout time.Duration) {
	r := vestigo.NewRouter()

	var monitoringRouter http.Handler = r
	monitoringRouter = httphandlers.TransactionAwareRequestLoggingHandler(log.StandardLogger(), monitoringRouter)
	monitoringRouter = httphandlers.HTTPMetricsHandler(metrics.DefaultRegistry, monitoringRouter)

	r.Get("/__health", healthService.HealthCheckHandleFunc())
	r.Get(status.GTGPath, status.NewGoodToGoHandler(healthService.GTG))
	r.Get(status.BuildInfoPath, status.BuildInfoHandler)

	for path, cfg := range rw.Paths {
		r.Get(path, resources.Read(db, cfg.Table, timeout))
		r.Put(path, resources.Write(db, cfg.Table, timeout))
		log.WithField("path", path).WithField("table", cfg.Table).Info("added r/w endpoint")
	}

	http.Handle("/", monitoringRouter)

	if apiYml != nil {
		apiEndpoint, err := api.NewAPIEndpointForFile(*apiYml)
		if err != nil {
			log.WithError(err).WithField("file", *apiYml).Warn("Failed to serve the API Endpoint for this service. Please validate the Swagger YML and the file location")
		} else {
			r.Get(api.DefaultPath, apiEndpoint.ServeHTTP)
		}
	}

	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatalf("Unable to start: %v", err)
	}
}
