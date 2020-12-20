package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/psanford/donutdb/donuthttp"
)

var addr = flag.String("addr", "127.0.0.1:3484", "Listen address")

func main() {
	flag.Parse()
	server := donuthttp.Server{
		AccessKey:       "DUMMYIDEXAMPLE",
		SecretAccessKey: "DUMMYEXAMPLEKEY",
		Region:          "us-west-2",
	}

	l, err := net.Listen("tcp", *addr)
	if err != nil {
		panic(err)
	}

	server.Listener = l

	server.Start()
	fmt.Printf("listening: %s\n", server.URL)

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	<-sigs
}
