package tkt

import (
	"fmt"
	"io/ioutil"
	"encoding/json"
	"net/http"
	"database/sql"
	"time"
	"os"
)

func Ping() string {
	return "pong"
}

func CheckErr(err error) {
	if err != nil {
		panic(err)
	}
}

func ResolvePanicMessage(r interface{}) string {
	return fmt.Sprint(r)
}


func Unmarshall(bytes []byte, object interface{}) {
	err := json.Unmarshal(bytes, object)
	if err != nil {
		panic(err)
	}
}

func ResolveBody(request *http.Request) ([]byte, error) {
	value := request.URL.Query().Get("body")
	if len(value) > 0 {
		return []byte(value), nil
	}
	bytes, err := ioutil.ReadAll(request.Body)
	if err != nil {
		return nil, err
	} else {
		return bytes, nil
	}

}

func ScanAll(rows *sql.Rows) []interface{} {
	result := make([]interface{}, 0)
	columns, err := rows.Columns()
	CheckErr(err)
	n := len(columns)
	r := 0
	references := make([]interface{}, n)
	pointers := make([]interface{}, n)
	for i := range references {
		pointers[i] = &references[i]
	}
	for rows.Next() {
		CheckErr(rows.Scan(pointers...))
		values := make([]interface{}, n)
		for i := range pointers {
			values[i] = *pointers[i].(*interface{})
		}
		result = append(result, values)
		r = r + 1
	}
	return result
}

func LoadDeployInfo() *DeployInfo {
	info := DeployInfo{}
	bytes, err := ioutil.ReadFile("deploy.json")
	CheckErr(err)
	err = json.Unmarshal(bytes, &info)
	CheckErr(err)
	return &info
}

func TruncDate(t time.Time) time.Time {
	y, m, d := t.Date()
	return time.Date(y, m, d, 0, 0, 0, 0, t.Location())
}

func Today() time.Time {
	return TruncDate(time.Now())
}


func FileExists(name string) bool {
	_, err := os.Stat(name)
	if err == nil {
		return true
	} else if os.IsNotExist(err) {
		return false
	} else {
		panic(err)
	}
}

func VarArr(elems ...interface{}) []interface{} {
	return elems
}



