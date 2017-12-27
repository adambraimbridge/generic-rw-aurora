# generic-rw-aurora

[![Circle CI](https://circleci.com/gh/Financial-Times/generic-rw-aurora/tree/master.png?style=shield)](https://circleci.com/gh/Financial-Times/generic-rw-aurora/tree/master)[![Go Report Card](https://goreportcard.com/badge/github.com/Financial-Times/generic-rw-aurora)](https://goreportcard.com/report/github.com/Financial-Times/generic-rw-aurora) [![Coverage Status](https://coveralls.io/repos/github/Financial-Times/generic-rw-aurora/badge.svg)](https://coveralls.io/github/Financial-Times/generic-rw-aurora)

Generic r/w app for Aurora

## How to build

```
go get -u github.com/kardianos/govendor
cd $GOPATH/src/github.com/Financial-Times
git clone git@github.com:Financial-Times/generic-rw-aurora.git
cd $GOPATH/src/github.com/Financial-Times/generic-rw-aurora
govendor sync
go build
```


## How to test

Run with `-short` to skip database integration tests.

To run database integration tests, you must set the environment variable `DB_TEST_URL` to a connection string for a MySQL database, with credentials that have privileges to create databases and users. The test cases will provision a test user `pac_test_user` and up-to-date schema in the database.


To test lcally run a local instance of mySql. use the version specified in cicelci config.
e.g. DB_TEST_URL=username:password@tcp(127.0.0.1:3306)/dbname

## Endpoints

For each `path` listed in the configuration file (see below), the service creates `GET` and `PUT` endpoints.

The application also has the standard `/__health`, `/__gtg` and `/__build-info` endpoints.

## Configuration

The application requires a YAML configuration file to map between HTTP endpoints and tables in the Aurora database.

The root object for the configuration is `paths`, which contains a mapping between URL paths and persistence stores. Paths may contain `:param-name` placeholders, which are recognised in the routing library. A path is mapped to a table, and a mapping of columns to expressions. The primary key column must also be specified.

The expressions for column values may contain the following syntax:
- `:name` extracts a value from the incoming request (a path or query string parameter)
- `@.name` extracts a value from the metadata for the incoming request. The names `timestamp` and `publishRef` are populated by the request time and the `X-Request-Id` HTTP header respectively.
- `$` extracts the entire request body
- `$.name` extracts a JSON path from the request body

For example:
```
paths:
  "/drafts/content/:id/annotations":
    table: draft_annotations
    columns:
      uuid: ":id"
      last_modified: "@.timestamp"
      publish_ref: "@.publishRef"
      body: "$"
    primaryKey: uuid
    hasConflictDetection: true
  "/published/content/:id/annotations":
    ...
```

## Write conflict detection 

It is possible to enable write conflict detection on a specific endpoint by 
setting to `true` the `hasConflictDetection` property in the YAML configuration file.

To use this feature, clients MUST set the special HTTP header `Previous-Document-Hash` 
when they update an existing document. 
The value of such header MUST be the hash of the current document body stored 
in the database that the client wants to update. 
The hash of the document is returned by each GET and PUT response in the `Document-Hash` 
HTTP header.
