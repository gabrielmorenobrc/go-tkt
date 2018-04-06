package tkt

import (
	"fmt"
	"net/http"
	"encoding/json"
	"strings"
)

func ParseParamOrBody(r *http.Request, o interface{}) error {
	s := r.URL.Query().Get("body")
	if len(s) > 0 {
		return json.NewDecoder(strings.NewReader(s)).Decode(o)
	} else {
		return json.NewDecoder(r.Body).Decode(o)
	}
}

func InterceptCORS(delegate func(w http.ResponseWriter, r *http.Request)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Add("Access-Control-Allow-Origin", "*")
		if r.Method == "OPTIONS" {
			header := r.Header.Get("Access-Control-Request-Headers")
			if len(header) > 0 {
				w.Header().Add("Access-Control-Allow-Headers", header)
			}
		} else {
			delegate(w, r)
		}
	}
}

func InterceptHandler(h http.Handler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		h.ServeHTTP(w, r)
	}
}

func InterceptPanic(delegate func(w http.ResponseWriter, r *http.Request)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		defer catchAll(w, r)
		delegate(w, r)
	}
}

func catchAll(writer http.ResponseWriter, response *http.Request) {
	if r := recover(); r != nil {
		ProcessPanic(r)
		http.Error(writer, fmt.Sprint(r), http.StatusInternalServerError)
	}
}
