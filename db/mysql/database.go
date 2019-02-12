package mysql

import (
	"database/sql"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"

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
	close_flag int32

	tables map[string]*tableInfo

	sync.RWMutex
	sync.WaitGroup
}

func New(user, passwd, addr, dbname string, qlen, save_count int) (IDataBaseInfo, error) {
	url := fmt.Sprintf("%s:%s@tcp(%s)/%s?charset=utf8", user, passwd, addr, dbname)
	db, err := sql.Open("mysql", url)
	if err != nil {
		return nil, err
	}

	d := &dataBaseInfo{
		tables:     make(map[string]*tableInfo),
		db:         db,
		queue_len:  qlen,
		save_count: save_count,
		dbname:     dbname,
	}
	return d, nil
}

func (d *dataBaseInfo) Close() {
	log.Info("database:%s closeing. ", d.dbname)

	atomic.AddInt32(&d.close_flag, 1)

	d.RLock()
	for _, v := range d.tables {
		v.tclose()
	}
	d.RUnlock()

	d.Wait()
	d.db.Close()
}

func (d *dataBaseInfo) Handle(body []byte) error {
	data := make(map[string]interface{})
	err := json.Unmarshal(body, &data)
	if err != nil {
		return err
	}
	v, ok := data["table_name"]
	if !ok {
		return fmt.Errorf("tablename should not emtpy, json:%s", string(body))
	}

	if reflect.TypeOf(v).String() != "string" {
		return fmt.Errorf("tablename type string, json:%s", string(body))
	}

	tname := v.(string)
	if tname == "" {
		return fmt.Errorf("tablename should not emtpy, json:%s", string(body))
	}

	t, err := d.getTable(tname, data)
	if err != nil {
		return err
	}
	return t.handle(data)
}

func (d *dataBaseInfo) getTable(tname string, data map[string]interface{}) (*tableInfo, error) {
	if atomic.LoadInt32(&d.close_flag) == 1 {
		return nil, fmt.Errorf("database:%s close table:%s", d.dbname, tname)
	}

	d.RLock()
	table, ok := d.tables[tname]
	if ok {
		d.RUnlock()
		return table, nil
	}
	d.RUnlock()

	d.Lock()
	defer d.Unlock()

	table, ok = d.tables[tname]
	if ok {
		return table, nil
	}

	table = newTable(tname, data, d)
	d.tables[tname] = table
	d.Add(1)

	return table, nil
}

func (d *dataBaseInfo) removeTable(tname string) {
	d.Lock()
	defer d.Unlock()

	if _, ok := d.tables[tname]; ok {
		delete(d.tables, tname)
		log.Info("remove table  database:%s tname:%s", d.dbname, tname)
	}
}
