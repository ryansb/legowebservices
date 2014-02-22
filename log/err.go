package log

func FatalIfErr(err error, args ...interface{}) {
	if err != nil {
		logging.print(fatalLog, append(args, err))
	}
}
