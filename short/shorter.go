package short

import (
	"bytes"
	"code.google.com/p/leveldb-go/leveldb"
	"code.google.com/p/leveldb-go/leveldb/db"
	"encoding/gob"
	"encoding/json"
	"flag"
	"github.com/codegangsta/martini"
	"github.com/ryansb/legowebservices/util"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"sync"
)

var counterKey = []byte("**global:count**")
var hitKey = "**hit**"
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
		log.Println("[ERROR]: Failure getting counter err:" + err.Error())
		panic(err)
	} else {
		c, err := strconv.Atoi(string(raw))
		count = int64(c)
		if err != nil {
			log.Println("[ERROR]: Failure converting count to int64")
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
	if err != nil {
		log.Println("[ERROR]: Failure reading request err:" + err.Error())
		panic(err)
	}
	var v map[string]interface{}
	err = json.Unmarshal(raw, &v)
	if err != nil {
		log.Println("[ERROR]: Failure decoding JSON err:" + err.Error() + " json:" + string(raw))
		panic(err)
	}
	if dest, ok := v["url"]; ok {
		shortSlug := base62.EncodeInt(incrCount(ldb))
		parsed, err := url.Parse(dest.(string))
		if err != nil {
			log.Println("[WARNING]: malformed URL:" + dest.(string) + " err:" + err.Error())
		}
		if parsed.Scheme == "" {
			dest = "http://" + dest.(string)
		}

		s := Shortened{
			Original: dest.(string),
			Short:    shortSlug,
		}

		err = saveShortened(s, ldb)
		if err != nil {
			log.Println("[ERROR]: Failure saving URL err:" + err.Error())
			panic(err)
		}
		out, _ := json.Marshal(map[string]interface{}{
			"Short":    s.Short,
			"Original": s.Original,
			"Full":     *base + s.Short,
			"HitCount": s.HitCount,
		})
		w.Write(out)
	} else {
		log.Println("[NOTICE]: No url field included in JSON")
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("Require 'url' field in request JSON"))
	}
}

func LongURL(short string, ldb *leveldb.DB) (*Shortened, error) {
	b, err := ldb.Get([]byte(short), nil)
	if err != nil {
		return nil, err
	}
	buf := bytes.NewBuffer([]byte(b))
	dec := gob.NewDecoder(buf)
	s := new(Shortened)
	err = dec.Decode(s)
	if err != nil {
		return nil, err
	}
	countVal, err := ldb.Get([]byte(hitKey+short), nil)
	if err != db.ErrNotFound && err != nil {
		return nil, err
	}
	s.HitCount, _ = strconv.Atoi(string(countVal))
	return s, nil
}

func saveShortened(s Shortened, ldb *leveldb.DB) error {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(s); err != nil {
		log.Println("[ERROR]: " + err.Error())
		return err
	}
	return ldb.Set([]byte(s.Short), buf.Bytes(), writeOpt)
}

func countHits(ldb *leveldb.DB) {
	var key string
	for {
		var count int
		key = <-hits
		v, err := ldb.Get([]byte(hitKey+key), nil)
		if err == db.ErrNotFound {
			log.Println("[ERROR]: Count not found for key")
			count = 1
		} else if err != nil {
			log.Println("[ERROR]: Failed to retrieve hitkey err:" + err.Error())
			count = 0
		} else {
			count, err := strconv.Atoi(string(v))
			if err != nil {
				log.Println("[ERROR]: Could not convert from string err:" + err.Error())
			}
			count += 1
		}
		err = ldb.Set([]byte(hitKey+key), []byte(strconv.Itoa(count)), writeOpt)
		if err != nil {
			log.Println("[ERROR]: Failed to write hitkey err:" + err.Error())
			continue
		}
		log.Println("[HIT]: key=" + key + " count=" + string(count))
		log.Println("[INFO]: wrote hitkey " + key)
	}
}

func NewShortener() *martini.Martini {
	flag.Parse()
	app := martini.New()

	levelDB, err := leveldb.Open("./lws_short_leveldb", &db.Options{VerifyChecksums: true})
	if err != nil {
		log.Println("[ERROR]: " + err.Error())
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
