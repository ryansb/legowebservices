package kv

import (
	"encoding/json"
	tiedot "github.com/HouzuoGuo/tiedot/db"
	"github.com/ryansb/legowebservices/log"
	. "github.com/ryansb/legowebservices/util/m"
)

func (q *Query) Equals(p Path, v interface{}) *Query {
	log.V(6).Infof("QueryBuilder: Path=%v Term=%v Value=%v", p, "Equals", v)
	q.q = append(q.q, M{"in": p, "eq": v})
	return q
}

func (q *Query) Between(p Path, start, end int64) *Query {
	log.V(6).Infof("QueryBuilder: Path=%v Between %d and %d", p, start, end)
	q.q = append(q.q, M{"in": p, "int from": start, "int to": end})
	return q
}

func (q *Query) Regexp(p Path, expr string) *Query {
	log.V(6).Infof("QueryBuilder: Path=%v Regexp=%s", p, expr)
	q.q = append(q.q, M{"in": p, "re": expr})
	return q
}

func (q *Query) Has(p Path) *Query {
	log.V(6).Infof("QueryBuilder: HasPath=%v", p)
	q.q = append(q.q, M{"has": p})
	return q
}

func (q *Query) All() (res ResultSet, err error) {
	r, err := q.eval()
	if err != nil {
		log.Errorf("Error executing kv.Query.All() query=%s err=%s", q.JSON(), err.Error())
		return
	}
	res = make(ResultSet)
	for id, _ := range r {
		v, err := q.read(id)
		if err != nil {
			log.Errorf("Failure reading id=%d err=%v", id, err)
		}
		res[id] = v
	}
	return
}

func (q *Query) OneInto(out interface{}) (uint64, error) {
	r, err := q.eval()
	if err != nil {
		log.Errorf("Error executing kv.Query.One() err=%s", err.Error())
		return 0, err
	}
	for k, v := range r {
		log.V(2).Infof("Found id=%d kv.Query.OneInto()", k)
		if _, err := q.col.Read(k, out); err != nil {
			log.Errorf("Failure reading id=%d err=%s", k, err.Error())
			return 0, err
		}
		log.V(2).Infof("Found id=%d val=%v for kv.Query.OneInto()", k, v)
		return k, nil
	}
	log.V(1).Infof("Nothing found for query=%s", q.JSON())
	q.col.ForAll(func(id uint64, doc map[string]interface{}) bool {
		log.V(1).Infof("id=%d val=%v", id, doc)
		return false
	})
	return 0, ErrNotFound
}

func (q *Query) One() (uint64, interface{}, error) {
	r, err := q.eval()
	if err != nil {
		log.Errorf("Error executing kv.Query.One() err=%s", err.Error())
		return 0, nil, err
	}
	for id, _ := range r {
		v, err := q.read(id)
		if err != nil {
			log.Errorf("Failure reading id=%d err=%v", id, err)
		}
		log.V(2).Infof("Found id=%d val=%v for kv.Query.One()", id, v)
		return id, v, nil
	}
	log.V(1).Infof("Nothing found for query=%v", q.JSON())
	return 0, nil, ErrNotFound
}

func (q *Query) Delete() (int, error) {
	res, err := q.eval()
	if err != nil {
		log.Errorf("Error executing kv.Query.Delete() query=%s err=%s", q.JSON(), err.Error())
		return -1, err
	}
	for id, _ := range res {
		q.col.Delete(id)
		log.V(6).Info("Deleted id=%d")
	}
	log.V(5).Info("Deleted %d objects for query=%s", len(res), q.JSON())
	return len(res), nil
}

func (q Query) JSON() string {
	j, err := json.Marshal(q.q)
	if err != nil {
		log.Error("Failure JSONifying query err=%s query=%v", err.Error(), q.q)
	}
	return string(j)
}

func (q *Query) read(id uint64) (interface{}, error) {
	v := new(interface{})
	if q.ReadLock == NoLock {
		q.col.ReadNoLock(id, v)
	} else if q.ReadLock == MustLock {
		q.col.Read(id, v)
	} else {
		log.Errorf("Read preference (NoLock or MustLock) not set for query=%s", q.JSON())
		return nil, ErrReadPreference
	}
	return *v, nil
}

func (q *Query) eval() (RawResultSet, error) {
	query := prepQuery(q.q)
	res := make(map[uint64]struct{})
	err := tiedot.EvalQuery(query, q.col, &res)
	return res, err
}

func prepQuery(q interface{}) (query interface{}) {
	j, err := json.Marshal(q)
	if err != nil {
		log.Errorf("Failure serializing query err=%v", err)
	}
	err = json.Unmarshal(j, &query)
	if err != nil {
		log.Errorf("Failure deserializing query err=%v", err)
	}
	return
}
