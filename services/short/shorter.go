package short

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"flag"
	"github.com/codegangsta/martini"
	"github.com/ryansb/legowebservices/encoding/base62"
	"github.com/ryansb/legowebservices/log"
	"github.com/ryansb/legowebservices/persist/kv"
	. "github.com/ryansb/legowebservices/util/m"
	"io/ioutil"
	"net/http"
	"net/url"
	"sync"
)

type Shortened struct {
	Original string
	Short    int64
	HitCount uint64
}

func (s Shortened) ToM() M {
	return M{
		"Original": s.Original,
		"Short":    s.Short,
		"HitCount": s.HitCount,
	}
}

type Counter struct {
	Count int64
}

func (c Counter) ToM() M {
	return M{"Count": c.Count}
}

var hits = make(chan string, 100)

var mu = new(sync.Mutex)

func incrCount(tde *kv.TiedotEngine) int64 {
	mu.Lock()
	defer mu.Unlock()
	counter := new(Counter)
	id, err := tde.Query(counterCollection).Has(kv.Path{"Count"}).OneInto(counter)
	if err == kv.ErrNotFound {
		log.Warning("Counter not found, saving new one.")
		_, err = tde.Insert(counterCollection, Counter{Count: 1})
		if err != nil {
			log.Errorf("Error saving new counter err=%s", err.Error())
		}
		r, err := tde.All(counterCollection)
		log.V(3).Infof("total of %d results=%v, err=%v", len(r), r, err)
		return 0
	}
	if err != nil {
		log.Error("Failure getting counter err:" + err.Error())
		panic(err)
	}
	counter.Count++
	err = tde.Update(counterCollection, id, counter)
	if err != nil {
		log.Error("Failure updating counter err:" + err.Error())
		panic(err)
	}
	return counter.Count
}

func incrHits(tde *kv.TiedotEngine, key string) uint64 {
	mu.Lock()
	defer mu.Unlock()
	var short Shortened
	id, err := tde.Query(counterCollection).Equals(
		kv.Path{"Short"},
		base62.DecodeString(key),
	).OneInto(short)
	if err == kv.ErrNotFound {
		log.Warningf("Short URL %s not found", key)
		return 0
	} else if err != nil {
		log.Errorf("Failure getting shortened URL key=%s err:", key, err.Error())
		return 0
	}

	short.HitCount++
	err = tde.Update(urlCollection, id, short)
	if err != nil {
		log.Error("Failure updating hitcount key=%s err=%s", key, err.Error())
		return 0
	}
	return short.HitCount
}

func newShort(w http.ResponseWriter, r *http.Request, tde *kv.TiedotEngine) {
	defer r.Body.Close()
	raw, err := ioutil.ReadAll(r.Body)
	log.FatalIfErr(err, "Failure reading request err:")
	var v M
	err = json.Unmarshal(raw, &v)
	log.FatalIfErr(err, "Failure decoding JSON json:"+string(raw)+" err:")
	if dest, ok := v["url"]; ok {
		count := incrCount(tde)
		shortSlug := base62.EncodeInt(count)
		parsed, err := url.Parse(dest.(string))
		if err != nil {
			log.Warning("Malformed URL:" + dest.(string) + " err:" + err.Error())
		}
		if parsed.Scheme == "" {
			dest = "http://" + dest.(string)
		}

		s := Shortened{
			Original: dest.(string),
			Short:    count,
		}

		err = saveShortened(s, tde)
		log.FatalIfErr(err, "Failure saving URL err:")
		out, _ := json.Marshal(map[string]interface{}{
			"Short":    s.Short,
			"Original": s.Original,
			"Full":     *base + shortSlug,
			"HitCount": s.HitCount,
		})
		w.Write(out)
	} else {
		log.Info("No url field included in JSON")
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("Require 'url' field in request JSON"))
	}
}

func encodeShortened(s Shortened) []byte {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	enc.Encode(s)
	return buf.Bytes()
}

func decodeShortened(v interface{}) (*Shortened, error) {
	o, _ := json.Marshal(v)
	s := new(Shortened)
	err := json.Unmarshal(o, s)
	if err != nil {
		return nil, err
	}
	return s, nil
}

func LongURL(short string, tde *kv.TiedotEngine) (*Shortened, error) {
	// ignore the ID for now, we don't really need it
	_, b, err := tde.Query("short.url").Equals(kv.Path{"Short"}, short).One()
	if err != nil {
		return nil, err
	}
	return decodeShortened(b)
}

func saveShortened(s Shortened, tde *kv.TiedotEngine) error {
	_, err := tde.Insert(urlCollection, s)
	return err
}

func countHits(tde *kv.TiedotEngine) {
	var key string
	for {
		key = <-hits
		newHitCount := incrHits(tde, key)
		log.V(3).Info("[HIT]: key=%s count=%d", key, newHitCount)
	}
}

func NewShortener(tde *kv.TiedotEngine) *martini.Martini {
	flag.Parse()
	app := martini.New()

	app.Map(tde)

	go countHits(tde)

	r := martini.NewRouter()
	r.Get("/", root)
	r.Post("/", newShort)
	r.Get("/:short", retrieve)
	r.Delete("/:short", remove)
	app.Action(r.Handle)
	return app
}
