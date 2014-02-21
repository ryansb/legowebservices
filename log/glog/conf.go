package glog

func SetV(v int) {
	if v < 0 {
		v = 0
	}
	logging.verbosity.setInt(v)
}

func SetVModule(vmod string) error {
	return logging.vmodule.Set(vmod)
}

func UseStderr(use bool) {
	logging.toStderr = true
}
