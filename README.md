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

To run database integration tests, you must set the environment variable `DB_TEST_URL` to the URL of an *empty* MySQL database. The test cases will provision the up-to-date schema in the database. As some tests check for an out-of-date schema, these may fail if your database is not empty to begin with.
