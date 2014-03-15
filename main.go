package main

// See LICENSE for licensing info

import (
	"flag"
	"github.com/codegangsta/martini"
	"github.com/ryansb/legowebservices/log"
	"github.com/ryansb/legowebservices/persist/kv"
	"github.com/ryansb/legowebservices/services/short"
	"net/http"
	"regexp"
)

var host = flag.String("host", "localhost", "Bind address to listen on")
var port = flag.String("port", ":3000", "Port to listen on")
var useShort = flag.Bool("short", false, "Whether to run the URL shortener")

func main() {
	log.UseStderr(true)
	log.SetV(9)
	flag.Parse()
	m := martini.New()
	m.Use(martini.Logger())
	m.Use(martini.Recovery())
	r := martini.NewRouter()

	tde := kv.NewTiedotEngine("./tiedotdb", []string{"short.url", "short.counter"}, kv.KeepIfExist)
	defer tde.Close()
	tde.AddIndex("short.url", kv.Path{"Short"})
	tde.AddIndex("short.counter", kv.Path{"Count"})

	if *useShort {
		log.Info("Starting LWS.short")
		s := short.NewShortener(tde)
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
