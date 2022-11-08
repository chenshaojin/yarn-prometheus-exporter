package log

import (
	"fmt"
	"github.com/jeanphorn/log4go"
	"os"
	"sync"
)

var (
	once   sync.Once
	metric *log4go.Filter
)

func GetIns() {
	once.Do(func() {

		log4go.LoadConfiguration("./log4go.json", "json")
		metric = log4go.LOGGER("metric")

	})
}

func Info(v ...any) {
	metric.Info(fmt.Sprint(v...))
}

func Fatal(v ...any) {
	metric.Error(fmt.Sprint(v...))
	os.Exit(1)
}
