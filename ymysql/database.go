package ymysql

import (
	"database/sql"
	"fmt"
	"sync"

	"github.com/adamluo159/ydata/log"
	_ "github.com/go-sql-driver/mysql"
	jsoniter "github.com/json-iterator/go"
)

var (
	json = jsoniter.ConfigCompatibleWithStandardLibrary
)

type IDataBaseInfo interface {
	Close()
	Handle([]byte) error
}

type dataBaseInfo struct {
	db         *sql.DB
	dbname     string
	queue_len  int
	save_count int

	tables map[string]*tableInfo

	sync.Mutex
	sync.WaitGroup
}

func New(user, passwd, addr, dbname string, qlen, save_count int) (IDataBaseInfo, error) {
	url := fmt.Sprintf("%s:%s@tcp(%s)/%s?charset=utf8", user, passwd, addr, dbname)
	db, err := sql.Open("mysql", url)
	if err != nil {
		return nil, err
	}

	dbase := &dataBaseInfo{
		tables:     make(map[string]*tableInfo),
		db:         db,
		queue_len:  qlen,
		save_count: save_count,
		dbname:     dbname,
	}
	return dbase, nil
}

func (dbase *dataBaseInfo) Close() {
	for _, v := range dbase.tables {
		v.tclose()
	}
	dbase.Wait()
	dbase.db.Close()
}

func (dbase *dataBaseInfo) Handle(body []byte) error {
	data := make(map[string]interface{})
	err := json.Unmarshal(body, &data)
	if err != nil {
		return err
	}
	v, ok := data["table_name"]
	if !ok {
		return fmt.Errorf("tablename should not emtpy, json:%s", string(body))
	}

	tname := v.(string)
	if tname == "" {
		return fmt.Errorf("tablename should not emtpy, json:%s", string(body))
	}
	t := dbase.getTable(tname, data)
	return t.handle(data)
}

func (dbase *dataBaseInfo) getTable(tname string, data map[string]interface{}) *tableInfo {
	dbase.Lock()
	defer dbase.Unlock()
	table, ok := dbase.tables[tname]
	if !ok {
		table = newTable(tname, data, dbase)
		dbase.tables[tname] = table
		dbase.Add(1)
	}
	return table
}

func (dbase *dataBaseInfo) removeTable(tname string) {
	if _, ok := dbase.tables[tname]; ok {
		delete(dbase.tables, tname)
		log.Info("remove table  database:%s tname:%s", dbase.dbname, tname)
	}
}
