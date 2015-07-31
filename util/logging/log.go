package logging

import (
	"log"
)

type Logger func(v ...interface{})

func NewLogger(enable bool) Logger {
	if enable {
		return func(v ...interface{}) {
			log.Println(v...)
		}
	} else {
		return func(v ...interface{}) {}
	}
}
