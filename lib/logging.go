package tkt

import (
	"log"
	"sync"
	"fmt"
	"io"
	"os"
)

var defaultLogger = log.New(os.Stdout, ":", log.Ldate | log.Lshortfile)

type LogWriter struct {
	io.Writer
	FileName    string
	MaxSize     int
	MaxFiles    int
	inizialized bool
	fileNumber  int
	totalBytes  int
	file        *os.File
	mux         *sync.Mutex
}

func (o *LogWriter)Write(p []byte) (n int, err error) {
	if !o.inizialized {
		o.mux = &sync.Mutex{}
		o.initialize()
	}
	o.mux.Lock()
	defer o.mux.Unlock()
	w, err := o.file.Write(p)
	if err != nil {
		return w, err
	}
	o.totalBytes += w
	if o.totalBytes >= o.MaxSize {
		o.file.Close()
		o.fileNumber ++
		if o.fileNumber == o.MaxFiles {
			o.fileNumber = 1
		}
		o.createFile()
	}
	return w, nil
}

func (o *LogWriter) initialize() {
	o.mux.Lock()
	defer o.mux.Unlock()
	if o.inizialized {
		return
	}
	o.fileNumber = 1
	o.createFile()
	o.inizialized = true
}

func (o *LogWriter)createFile() {
	var err error
	name := fmt.Sprintf(o.FileName + ".%d", o.fileNumber)

	o.file, err = os.Create(name)
	CheckErr(err)
	CheckErr(os.Chmod(name, 0644))

	o.totalBytes = 0
}

type NullWriter struct {
	io.Writer
}

func (o *NullWriter)Write(p []byte) (n int, err error) {
	return len(p), nil
}

type Loggers struct {
	output io.Writer
	loggerMap map[string]*log.Logger
	nullLogger *log.Logger
}


func (o*Loggers)Config(fileName string, maxSize int, maxFiles int, console bool, tags ...string) {
	w := LogWriter{FileName:fileName, MaxFiles:maxFiles, MaxSize:maxSize}
	if console {
		o.output = io.MultiWriter(&w, os.Stdout)
	} else {
		o.output = &w
	}

	o.loggerMap = make(map[string]*log.Logger)
	for i := range tags {
		prefix := tags[i]
		o.loggerMap[prefix] = log.New(o.output, prefix + ": ", log.Ldate|log.Ltime|log.Lshortfile)
	}
	nullWriter := NullWriter{}
	o.nullLogger = log.New(&nullWriter, "", 0)
}

func (o *Loggers) Log(prefix string) *log.Logger {
	if o.loggerMap == nil {
		return defaultLogger
	} else if l, ok := o.loggerMap[prefix]; ok {
		return l
	} else {
		return o.nullLogger
	}
}

var loggers Loggers

func ConfigLoggers(fileName string, maxSize int, maxFiles int, console bool, tags ...string) {
	loggers.Config(fileName, maxSize, maxFiles, console, tags...)
}

func Logger(tag string) *log.Logger {
	return loggers.Log(tag)
}
