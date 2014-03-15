package kv

import (
	. "github.com/ryansb/legowebservices/util/m"
)

type Insertable interface {
	ToM() M
}
