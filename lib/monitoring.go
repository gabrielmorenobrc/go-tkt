package tkt

import "time"

type DeployInfo struct {
	Tag string `json:"tag"`
	Env string `json:"env"`
}

type Instance struct {
	ServiceName string     `json:"serviceName"`
	DeployInfo  DeployInfo `json:"deployInfo""`
}

func (o *Instance)BuildKey() string {
	return o.ServiceName + "." + o.DeployInfo.Tag + "." + o.DeployInfo.Env
}

type ErrorInfo struct {
	Mesaage string `json:"message"`
	Time time.Time `json:"time"`
}

type InstanceInfo struct {
	Instance Instance `json:"instance"`
	LastErrorInfo *ErrorInfo `json:"lastErrorInfo"`
}
