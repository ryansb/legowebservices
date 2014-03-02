package kv

import (
	"encoding/json"
	tiedot "github.com/HouzuoGuo/tiedot/db"
	"github.com/ryansb/legowebservices/log"
	. "github.com/ryansb/legowebservices/util/m"
)

func (q *Query) Equals(p Path, v interface{}) *Query {
	log.V(6).Info("QueryBuilder: Path=%v Term=%v Value=%v", p, "Equals", v)
	q.q = append(q.q, M{"in": p, "eq": v})
	return q
}

func (q *Query) Between(p Path, start, end int64) *Query {
	log.V(6).Info("QueryBuilder: Path=%v Between %d and %d", p, start, end)
	q.q = append(q.q, M{"in": p, "int from": start, "int to": end})
	return q
}
func (q *Query) Regexp(p Path, expr string) *Query {
	log.V(6).Info("QueryBuilder: Path=%v Regexp=%s", p, expr)
	q.q = append(q.q, M{"in": p, "re": expr})
	return q
}

func (q *Query) All() (ResultSet, error) {
	r := make(map[uint64]struct{})
	if err := tiedot.EvalQuery("all", q.col, &r); err != nil {
		log.Error("Error executing kv.Query.All() err=%s", err.Error())
		return nil, err
	}
	return r, nil
}

func (q Query) JSON() string {
	j, err := json.Marshal(q.q)
	if err != nil {
		log.Error("Failure JSONifying query err=%s query=%v", err.Error(), q.q)
	}
	return string(j)
}

func (q *Query) One() (uint64, *struct{}, error) {
	r := make(map[uint64]struct{})
	if err := tiedot.EvalQuery("all", q.col, &r); err != nil {
		log.Error("Error executing kv.Query.One() err=%s", err.Error())
		return 0, nil, err
	}
	for k, v := range r {
		log.V(2).Info("Found id=%d val=%v for kv.Query.One()", k, v)
		return k, &v, nil
	}
	log.V(1).Info("Nothing found for query=", q.JSON())
	return 0, nil, ErrNotFound
}
