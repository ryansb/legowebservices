package main

import (
	"flag"
	"github.com/ryansb/legowebservices/services/short"
	"net/http"
)

var port = flag.String("p", ":3000", "Port you want to listen on, defaults to 3000")

func main() {
	flag.Parse()
	m := short.NewShortener()
	http.ListenAndServe(*port, m)
}
