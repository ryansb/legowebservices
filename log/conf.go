package log

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

func DevelDefaults() {
	UseStderr(true)
	SetV(5)
}

func ProdDefaults() {
	UseStderr(false)
	SetV(2)
}
