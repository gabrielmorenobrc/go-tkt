package tkt

import (
	"log"
	"database/sql"
	_ "github.com/lib/pq"
	_ "github.com/denisenkom/go-mssqldb"
	"reflect"
	"strings"
	"fmt"
	"time"
)

type DatabaseConfig struct {
	DatabaseDriver  string `json:"databaseDriver"`
	DatasourceName  string `json:"datasourceName"`
	MaxIdleConns    *int   `json:"maxIdleConns"`
	MaxOpenConns    *int   `json:"maxOpenConns"`
	MaxConnLifetime *int   `json:"maxConnLifetime"`
}

type Transactional interface {
	Execute()
	SetTx(src *sql.Tx)
	Tx() *sql.Tx
	SetDb(src *sql.DB)
	Db() *sql.DB
}

type TransactionalImpl struct {
	tx *sql.Tx
	db *sql.DB
}

func (m *TransactionalImpl) SetTx(tx *sql.Tx) {
	m.tx = tx
}

func (m *TransactionalImpl) Tx() *sql.Tx {
	return m.tx
}

func (m *TransactionalImpl) SetDb(db *sql.DB) {
	m.db = db
}

func (m *TransactionalImpl) Db() *sql.DB {
	return m.db
}

func ExecuteTransactional(config DatabaseConfig, transactional Transactional) {
	db := OpenDB(config)
	defer closeDB(db)
	tx, err := db.Begin()
	if err != nil {
		panic(err)
	}
	defer rollbackOnPanic(tx)
	transactional.SetDb(db)
	transactional.SetTx(tx)
	transactional.Execute()
	tx.Commit()
}

func ExecuteInContext(context Transactional, transactional Transactional) {
	transactional.SetDb(context.Db())
	transactional.SetTx(context.Tx())
	transactional.Execute()
}

func ExecuteTransactionalFunc(config DatabaseConfig, callback func(db *sql.DB, tx *sql.Tx, args ...interface{}) interface{}, args ...interface{}) interface{} {
	db := OpenDB(config)
	defer closeDB(db)
	tx, err := db.Begin()
	if err != nil {
		panic(err)
	}
	defer rollbackOnPanic(tx)
	r := callback(db, tx, args)
	tx.Commit()
	return r
}

func closeDB(db *sql.DB) {
	db.Close()
}

func rollbackOnPanic(tx *sql.Tx) {
	if r := recover(); r != nil {
		var message = ResolvePanicMessage(r)
		log.Print(message)
		tx.Rollback()
		panic(r)
	}
}

func OpenDB(config DatabaseConfig) *sql.DB {
	db, err := sql.Open(config.DatabaseDriver, config.DatasourceName)
	if err != nil {
		panic(err)
	}
	if config.MaxIdleConns != nil {
		db.SetMaxIdleConns(*config.MaxIdleConns)
	}
	if config.MaxOpenConns != nil {
		db.SetMaxOpenConns(*config.MaxOpenConns)
	}
	if config.MaxConnLifetime != nil {
		db.SetConnMaxLifetime(time.Duration(*config.MaxConnLifetime) * time.Second)
	}
	return db
}

func QueryStructN(tx *sql.Tx, template interface{}, sql string, queryParams ...interface{}) interface{} {
	stmt, e := tx.Prepare(sql)
	CheckErr(e)
	defer stmt.Close()
	return QueryStructNStmt(stmt, template, queryParams...)
}

func QueryStructNStmt(stmt *sql.Stmt, template interface{}, queryParams ...interface{}) interface{} {
	objectType := reflect.TypeOf(template)
	r, e := stmt.Query(queryParams...)
	CheckErr(e)
	cols, e := r.Columns()
	CheckErr(e)
	count := objectType.NumField()
	if len(cols) > count {
		panic("Result set column count greater than struct field count")
	}
	fields := buildFieldListByNames(objectType, cols)
	buffer := make([]interface{}, len(cols))
	for i := range cols {
		buffer[i] = reflect.New(fields[i].Type).Interface()
	}
	arr := reflect.MakeSlice(reflect.SliceOf(objectType), 0, 0)
	for r.Next() {
		CheckErr(r.Scan(buffer...))
		object := reflect.New(objectType).Elem()
		for i := range cols {
			f := fields[i]
			of := object.FieldByIndex(f.Index)
			v := buffer[i]
			of.Set(reflect.ValueOf(v).Elem())
		}
		arr = reflect.Append(arr, object)
	}
	r.Close()
	return arr.Interface()
}

func buildFieldListByNames(objectType reflect.Type, cols []string) []reflect.StructField {
	fieldMap := make(map[string]reflect.StructField)
	for i := 0; i < objectType.NumField(); i++ {
		f := objectType.Field(i)
		var k string
		if _, ok := f.Tag.Lookup("transient"); ok {
			Logger("persistence").Println("Bypassing transient field %s", f.Name)
		} else if t, ok := f.Tag.Lookup("column"); ok {
			k = strings.ToLower(t)
		} else {
			k = strings.ToLower(f.Name)
		}
		fieldMap[k] = f
	}
	fields := make([]reflect.StructField, len(cols))
	for i := range cols {
		col := cols[i]
		k := strings.ToLower(col)
		f, ok := fieldMap[k]
		if !ok {
			panic(fmt.Sprintf("Struct field not found for column %s", k))
		}
		fields[i] = f
	}
	return fields
}

func QueryStruct(tx *sql.Tx, template interface{}, sql string, queryParams ...interface{}) interface{} {
	stmt, err := tx.Prepare(sql)
	CheckErr(err)
	return QueryStructStmt(stmt, template, queryParams...)
}

func QueryStructStmt(stmt *sql.Stmt, template interface{}, queryParams ...interface{}) interface{} {
	objectType := reflect.TypeOf(template)
	fields := make([]reflect.StructField, objectType.NumField())
	for i := 0; i < objectType.NumField(); i++ {
		fields[i] = objectType.Field(i)
	}
	r, e := stmt.Query(queryParams...)
	CheckErr(e)
	count := objectType.NumField()
	cols, e := r.Columns()
	CheckErr(e)
	if len(cols) > count {
		panic("Result set column count greater than struct field count")
	}
	buffer := make([]interface{}, len(fields))
	for i := range fields {
		buffer[i] = reflect.New(fields[i].Type).Interface()
	}
	arr := reflect.MakeSlice(reflect.SliceOf(objectType), 0, 0)
	for r.Next() {
		CheckErr(r.Scan(buffer...))
		object := reflect.New(objectType).Elem()
		for i := range fields {
			f := fields[i]
			if _, ok := f.Tag.Lookup("transient"); ok {
				Logger("persistence").Println("Bypassing transient field %s", f.Name)
			} else {
				fi := object.Field(i)
				v := buffer[i]
				fi.Set(reflect.ValueOf(v).Elem())
			}
		}
		arr = reflect.Append(arr, object)
	}
	r.Close()
	return arr.Interface()
}

type FieldInfo struct {
	HolderType  *reflect.Type
	StructField *reflect.StructField
}

func QueryStructs(tx *sql.Tx, templates []interface{}, sql string, queryParams ...interface{}) [][]interface{} {
	objectTypes := buildObjectTypes(templates)
	r, e := tx.Query(sql, queryParams...)
	CheckErr(e)
	cols, e := r.Columns()
	CheckErr(e)
	fieldInfoList := buildFieldInfoList(objectTypes)
	if len(cols) != len(fieldInfoList) {
		panic("Result set column count differs from structs field count")
	}
	buffer := make([]interface{}, len(fieldInfoList))
	for i := range fieldInfoList {
		sf := fieldInfoList[i].StructField
		buffer[i] = reflect.New(sf.Type).Interface()
	}
	arr := make([][]interface{}, 0)
	for r.Next() {
		instancesElemns := make([]reflect.Value, len(objectTypes))
		for i := range objectTypes {
			t := objectTypes[i]
			e := reflect.New(t).Elem()
			instancesElemns[i] = e
		}
		CheckErr(r.Scan(buffer...))
		ii := -1
		var t *reflect.Type
		for i := range fieldInfoList {
			fi := fieldInfoList[i]
			if t != fi.HolderType {
				ii++
				t = fi.HolderType
			}
			if _, ok := fi.StructField.Tag.Lookup("transient"); ok {
				Logger("persistence").Println("Bypassing transient field %s", fi.StructField.Name)
			} else {
				of := instancesElemns[ii].FieldByIndex(fi.StructField.Index)
				v := buffer[i]
				of.Set(reflect.ValueOf(v).Elem())
			}
		}
		instances := make([]interface{}, len(objectTypes))
		for i := range instancesElemns {
			instances[i] = instancesElemns[i].Interface()
		}
		arr = append(arr, instances)
	}
	r.Close()
	return arr
}

func buildFieldInfoList(types []reflect.Type) []FieldInfo {
	r := make([]FieldInfo, 0)
	for i := range types {
		t := types[i]
		for j := 0; j < t.NumField(); j++ {
			f := t.Field(j)
			fi := FieldInfo{}
			fi.HolderType = &t
			fi.StructField = &f
			r = append(r, fi)
		}
	}
	return r
}

func buildObjectTypes(templates []interface{}) []reflect.Type {
	types := make([]reflect.Type, len(templates))
	for i := range templates {
		types[i] = reflect.TypeOf(templates[i])
	}
	return types
}

func ExecStruct(tx *sql.Tx, sql string, data interface{}) {
	stmt, e := tx.Prepare(sql)
	CheckErr(e)
	defer stmt.Close()
	ExecStructStmt(stmt, data)
}

func ExecStructStmt(stmt *sql.Stmt, data interface{}) {
	ExecStructStmtOff(stmt, data, 0)
}

func ExecStructStmtOff(stmt *sql.Stmt, data interface{}, offset int) {
	objectType := reflect.TypeOf(data)
	fields := make([]reflect.StructField, objectType.NumField()-offset)
	for i := 0; i < len(fields); i++ {
		fields[i] = objectType.Field(i + offset)
	}
	buffer := make([]interface{}, len(fields))
	value := reflect.ValueOf(data)
	for i := range fields {
		buffer[i] = value.Field(i + offset).Interface()
	}
	_, err := stmt.Exec(buffer...)
	CheckErr(err)
}

func QuerySingleton(tx *sql.Tx, fields []interface{}, sql string, args ...interface{}) bool {
	r, err := tx.Query(sql, args...)
	CheckErr(err)
	defer r.Close()
	if r.Next() {
		CheckErr(r.Scan(fields...))
		return true
	} else {
		return false
	}

}

func QuerySingletonStmt(stmt *sql.Stmt, fields []interface{}, args ...interface{}) bool {
	r, err := stmt.Query(args...)
	CheckErr(err)
	defer r.Close()
	if r.Next() {
		CheckErr(r.Scan(fields...))
		return true
	} else {
		return false
	}
}

func Query(tx *sql.Tx, sql string, args ...interface{}) *sql.Rows {
	r, err := tx.Query(sql, args...)
	CheckErr(err)
	return r
}

func Scan(r *sql.Rows, vars ...interface{}) {
	CheckErr(r.Scan(vars...))
}
