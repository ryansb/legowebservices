package main

import (
	"flag"
	"github.com/codegangsta/martini"
	"github.com/ryansb/legowebservices/short"
	"log"
	"net/http"
	"regexp"
)

var host = flag.String("host", "localhost", "Bind address to listen on")
var port = flag.String("port", ":3000", "Port to listen on")
var useShort = flag.Bool("short", false, "Whether to run the URL shortener")

func main() {
	flag.Parse()
	m := martini.New()
	m.Use(martini.Logger())
	m.Use(martini.Recovery())
	r := martini.NewRouter()

	if *useShort {
		log.Println("[INFO]: Starting LWS.short")
		s := short.NewShortener()
		r.Any("/s", stripper("/s"), s.ServeHTTP)
		r.Any("/s/.*", stripper("/s"), s.ServeHTTP)
	}

	m.Action(r.Handle)
	http.ListenAndServe(*port, m)
}
func stripper(p string) func(http.ResponseWriter, *http.Request) {
	re := regexp.MustCompile("^" + p)
	return func(w http.ResponseWriter, r *http.Request) {
		n := re.ReplaceAllString(r.URL.Path, "")
		if len(n) == 0 {
			r.URL.Path = "/"
		} else {
			r.URL.Path = n
		}
	}
}
