package short

import (
	"github.com/codegangsta/martini"
	"github.com/ryansb/legowebservices/encoding/base62"
	"github.com/ryansb/legowebservices/log"
	"github.com/ryansb/legowebservices/persist/kv"
	. "github.com/ryansb/legowebservices/util/m"
	"net/http"
)

var urlCollection = "short.url"
var counterCollection = "short.counter"

var Collections = []kv.CollectionParams{
	kv.CollectionParams{
		Name:       counterCollection,
		Partitions: 1,
		Indices:    []kv.Path{kv.Path{"Count"}},
	},
	kv.CollectionParams{
		Name:       urlCollection,
		Partitions: 1,
		Indices:    []kv.Path{kv.Path{"Short"}},
	},
}

func root(w http.ResponseWriter, r *http.Request) (int, string) {
	log.V(3).Info("Served Homepage")
	return 200, ("Welcome to legowebservices.short URL shortener service.\n" +
		"POST to this URL with JSON matching {\"url\":\"some.long.url.com\"}\n")
}

func retrieve(w http.ResponseWriter, r *http.Request, tde *kv.TiedotEngine, params martini.Params) {
	short := params["short"]
	domain, err := LongURL(short, tde)
	if err == kv.ErrNotFound {
		log.V(1).Info("Path /" + short + " not found")
		w.WriteHeader(http.StatusNotFound)
		return
	}
	if len(domain.Original) > 0 {
		log.V(3).Info("[INFO]: Served /" + short + " redirect to " + domain.Original)
		http.Redirect(w, r, domain.Original, http.StatusFound)
		hits <- short
		return
	}
	log.Error("retrieving long URL err:" + err.Error())
	w.WriteHeader(500)
	if err != nil {
		w.Write(M{"error": err.Error()}.JSON())
	}
}

func remove(w http.ResponseWriter, r *http.Request, tde *kv.TiedotEngine, params martini.Params) (int, []byte) {
	short := params["short"]
	_, err := tde.Query(urlCollection).Equals(kv.Path{"Short"}, base62.DecodeString(short)).Delete()
	if err != nil {
		log.Error("Failure deleting URL /" + short + " err:" + err.Error())
		return 500, M{
			"message": "Could not delete URL /" + short,
			"error":   err.Error(),
		}.JSON()
	}
	log.V(1).Info("Deleted URL /" + short)
	return 200, M{
		"deleted": M{"short": short},
	}.JSON()
}
