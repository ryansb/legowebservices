package short

import (
	"bytes"
	"code.google.com/p/leveldb-go/leveldb"
	"code.google.com/p/leveldb-go/leveldb/db"
	"encoding/gob"
	"encoding/json"
	"flag"
	"github.com/codegangsta/martini"
	"github.com/ryansb/legowebservices/encoding/base62"
	"github.com/ryansb/legowebservices/log"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"sync"
)

var counterKey = []byte("**global:count**")
var writeOpt = &db.WriteOptions{Sync: true}

type Shortened struct {
	Original string
	Short    string
	HitCount int
}

var hits = make(chan string, 100)

var mu = new(sync.Mutex)

func incrCount(ldb *leveldb.DB) int64 {
	mu.Lock()
	defer mu.Unlock()
	var count int64
	raw, err := ldb.Get(counterKey, nil)
	if err == db.ErrNotFound {
		count = 0
	} else if err != nil {
		log.Error("Failure getting counter err:" + err.Error())
		panic(err)
	} else {
		c, err := strconv.Atoi(string(raw))
		count = int64(c)
		if err != nil {
			log.Error("Failure converting count to int64")
			panic(err)
		}
	}
	count += 1
	err = ldb.Set(counterKey, []byte(strconv.Itoa(int(count))), writeOpt)
	if err != nil {
		panic(err)
	}
	return count
}

func newShort(w http.ResponseWriter, r *http.Request, ldb *leveldb.DB) {
	defer r.Body.Close()
	raw, err := ioutil.ReadAll(r.Body)
	log.FatalIfErr(err, "Failure reading request err:")
	var v map[string]interface{}
	err = json.Unmarshal(raw, &v)
	log.FatalIfErr(err, "Failure decoding JSON json:"+string(raw)+" err:")
	if dest, ok := v["url"]; ok {
		shortSlug := base62.EncodeInt(incrCount(ldb))
		parsed, err := url.Parse(dest.(string))
		if err != nil {
			log.Warning("Malformed URL:" + dest.(string) + " err:" + err.Error())
		}
		if parsed.Scheme == "" {
			dest = "http://" + dest.(string)
		}

		s := Shortened{
			Original: dest.(string),
			Short:    shortSlug,
		}

		err = saveShortened(s, ldb)
		log.FatalIfErr(err, "Failure saving URL err:")
		out, _ := json.Marshal(map[string]interface{}{
			"Short":    s.Short,
			"Original": s.Original,
			"Full":     *base + s.Short,
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

func decodeShortened(raw []byte) (*Shortened, error) {
	buf := bytes.NewBuffer([]byte(raw))
	dec := gob.NewDecoder(buf)
	s := new(Shortened)
	err := dec.Decode(s)
	if err != nil {
		return nil, err
	}
	return s, nil
}

func LongURL(short string, ldb *leveldb.DB) (*Shortened, error) {
	b, err := ldb.Get([]byte(short), nil)
	if err != nil {
		return nil, err
	}
	return decodeShortened(b)
}

func saveShortened(s Shortened, ldb *leveldb.DB) error {
	return ldb.Set([]byte(s.Short), encodeShortened(s), writeOpt)
}

func countHits(ldb *leveldb.DB) {
	var key string
	for {
		key = <-hits
		b, err := ldb.Get([]byte(key), nil)

		if err == db.ErrNotFound {
			log.Error("Count not found for key key:" + key)
			continue
		} else if err != nil {
			log.Error("Failed to retrieve hitkey err:" + err.Error())
			continue
		}

		s, err := decodeShortened(b)
		if err != nil {
			log.Error("Could not convert from string err:" + err.Error())
			continue
		}
		s.HitCount += 1

		err = ldb.Set([]byte(key), encodeShortened(*s), writeOpt)
		if err != nil {
			log.Error("Failed to write hitkey err:" + err.Error())
			continue
		}
		log.V(3).Info("[HIT]: key=%s count=%d", key, s.HitCount)
	}
}

func NewShortener() *martini.Martini {
	flag.Parse()
	app := martini.New()

	levelDB, err := leveldb.Open("./lws_short_leveldb", &db.Options{VerifyChecksums: true})
	if err != nil {
		log.Error("Failure opening leveldb err:" + err.Error())
	}
	defer levelDB.Close()

	app.Map(levelDB)

	go countHits(levelDB)

	r := martini.NewRouter()
	r.Get("/", root)
	r.Post("/", newShort)
	r.Get("/:short", retrieve)
	r.Delete("/:short", remove)
	app.Action(r.Handle)
	return app
}
