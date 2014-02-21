package util

import (
	"flag"
	"github.com/golang/glog"
)

func init() {
	flag.Parse()
	if glog.V(1) {
		glog.Error("lol")
	}
	glog.V(1).Info("LOLINFO1")
	glog.V(2).Info("LOLINFO2")
	glog.Error("LOLINFO")
}
