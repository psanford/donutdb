package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/psanford/donutdb/donuthttp"
)

func main() {
	server := donuthttp.NewServer()
	server.AccessKey = "DUMMYIDEXAMPLE"
	server.SecretAccessKey = "DUMMYEXAMPLEKEY"
	server.Region = "us-west-2"

	server.Start()
	fmt.Printf("listening: %s\n", server.URL)

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	<-sigs
}
