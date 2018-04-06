package tkt

import (
	"sync"
	"database/sql"
)

type Sequence struct {
	Name   string
	LastId int64
}

type Sequences struct {
	Map    map[string]*Sequence
	Config DatabaseConfig
	mux    sync.Mutex
}

func (o *Sequences) Next(name string) int64 {
	o.mux.Lock()
	defer o.mux.Unlock()
	seq, ok := o.Map[name]
	if !ok {
		seq = &Sequence{name, 0}
		o.initSequence(seq)
		o.Map[name] = seq
	}
	seq.LastId = seq.LastId + 1
	return seq.LastId

}

func (o *Sequences) initSequence(sequence *Sequence) {
	db := OpenDB(o.Config)
	defer db.Close()
	tx, err := db.Begin()
	CheckErr(err)
	result, err := tx.Query("select max(id) from " + sequence.Name)
	CheckErr(err)
	defer result.Close()
	result.Next()
	var r sql.NullInt64
	result.Scan(&r)
	var id = int64(0)
	if r.Valid {
		id = r.Int64
	}
	sequence.LastId = id
}

func NewSequences(config DatabaseConfig) *Sequences {
	seqs := Sequences{make(map[string]*Sequence, 0), config, sync.Mutex{}}
	return &seqs
}
