package tkt

import (
	"log"
	"runtime/debug"
	"encoding/json"
	"io/ioutil"
	"sync"
	"net/http"
	"strconv"
	"time"
	"github.com/nats-io/go-nats"
	"fmt"
	"flag"
)

type PanicInfo struct {
	Seqno      int
	Message    interface{}
	PanicTime  time.Time
	StackTrace string
}

type MonitorableErrors struct {
	LastSeqno int
	mux       sync.Mutex
}

var monitorableErrors = MonitorableErrors{mux: sync.Mutex{}, LastSeqno: 0}

func (m *MonitorableErrors) Inc() int {
	m.mux.Lock()
	defer m.mux.Unlock()
	m.LastSeqno = m.LastSeqno + 1
	return m.LastSeqno
}

func ProcessPanic(intf interface{}) {
	defer catchErrInternal()
	seqno := monitorableErrors.Inc()
	log.Print(intf)
	stackTrace := string(debug.Stack())
	log.Print(stackTrace)
	panicInfo := PanicInfo{Seqno: seqno, Message: fmt.Sprintf("%s",intf), StackTrace: stackTrace, PanicTime: time.Now()}
	storePanicInfo(panicInfo)
}

func catchErrInternal() {
	if r := recover(); r != nil {
		log.Print(r)
	}
}

func storePanicInfo(info PanicInfo) {
	bytes, err := json.Marshal(info)
	if err == nil {
		s := strconv.Itoa(info.Seqno) + ".err"
		err = ioutil.WriteFile(s, bytes, 0777)
	}
	if err != nil {
		log.Print(err)
	}
}

func loadLastPanicInfo() *PanicInfo {
	if monitorableErrors.LastSeqno != 0 {
		var info = PanicInfo{}
		s := strconv.Itoa(monitorableErrors.LastSeqno)
		bytes, err := ioutil.ReadFile(s + ".err")
		CheckErr(err)
		err = json.Unmarshal(bytes, &info)
		CheckErr(err)
		return &info
	} else {
		return nil
	}
}

type LastErrorInfoResult struct {
	Info *PanicInfo
}

type MonitorableApi struct {
	Prefix string
}

func (o *MonitorableApi) Serve() {
	http.HandleFunc(o.Prefix+"/lastErrorInfo", InterceptPanic(o.LastErrorInfoHandler))
	http.HandleFunc(o.Prefix+"/deployInfo", InterceptPanic(o.DeployInfoHandler))
}

func (o *MonitorableApi) LastErrorInfoHandler(w http.ResponseWriter, r *http.Request) {
	panicInfo := loadLastPanicInfo()
	result := LastErrorInfoResult{}
	result.Info = panicInfo
	err := json.NewEncoder(w).Encode(result)
	CheckErr(err)
}

func (o *MonitorableApi) DeployInfoHandler(w http.ResponseWriter, r *http.Request) {
	info := LoadDeployInfo()
	CheckErr(json.NewEncoder(w).Encode(info))
}


var signalingSubject = flag.String("sigsub", "", "Signaling subject")

type Signaler struct {
	NatsUrl     string
	NatsSubject string `json:"natsSubject"`
	ServiceName string `json:"serviceName"`
	connection  *nats.Conn
}

func (o *Signaler) Start() error {
	log.Printf("signaling subject is %s", o.resolveSignalingsubject())

	nc, err := nats.Connect(o.NatsUrl)
	if err != nil {
		return err
	}
	o.connection = nc
	go o.signal()
	return nil
}

func (o *Signaler) signal() {
	defer o.later()
	info := InstanceInfo{}
	info.Instance = Instance{}
	info.Instance.ServiceName = o.ServiceName
	info.Instance.DeployInfo = *LoadDeployInfo()
	panicInfo := loadLastPanicInfo()
	if panicInfo != nil {
		errorInfo := ErrorInfo{}
		errorInfo.Mesaage = fmt.Sprint(panicInfo.Message)
		errorInfo.Time = panicInfo.PanicTime
		info.LastErrorInfo = &errorInfo
	}
	bytes, err := json.Marshal(&info)
	CheckErr(err)
	o.connection.Publish(o.resolveSignalingsubject(), bytes)

	time.Sleep(time.Second * 10)
}

func (o *Signaler) later() {
	if r := recover(); r != nil {
		log.Print(r)
	} else {
		go o.signal()
	}
}

func (o *Signaler)resolveSignalingsubject() string {
	if *signalingSubject == "" {
		return o.NatsSubject
	} else {
		return *signalingSubject
	}
}

func NewSignaler(url string, subject string) *Signaler {
	s := Signaler{NatsUrl:url, NatsSubject:subject}
	return &s
}
