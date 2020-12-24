package main

import (
	"database/sql"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"

	_ "github.com/mattn/go-sqlite3"
	"github.com/psanford/donutdb"
	"github.com/psanford/donutdb/donuthttp"
	"github.com/psanford/donutdb/logger"
)

var addr = flag.String("addr", "127.0.0.1:3484", "Listen address")
var dbConn = flag.String("db", ":memory:?cache=shared", "sqlite db connection string (or path)")
var logPayloads = flag.Bool("log-payloads", false, "Log http request payloads")

func main() {
	flag.Parse()
	l, err := net.Listen("tcp", *addr)
	if err != nil {
		panic(err)
	}

	sqldb, err := sql.Open("sqlite3", *dbConn)
	if err != nil {
		panic(err)
	}
	db, err := donutdb.New(sqldb)
	if err != nil {
		panic(err)
	}

	server := donuthttp.Server{
		AccessKey:       "DUMMYIDEXAMPLE",
		SecretAccessKey: "DUMMYEXAMPLEKEY",
		Region:          "us-west-2",
		Listener:        l,
		DB:              db,
		Logger:          logger.StdoutLogger,
		LogLevel:        logger.LogHTTPRequests,
	}

	if *logPayloads {
		server.LogLevel |= logger.LogHTTPPayloads
	}

	server.Start()
	fmt.Printf("listening: %s\n", server.URL)

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	<-sigs
}
