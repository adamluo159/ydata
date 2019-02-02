package ymysql

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/adamluo159/ydata/log"
)

type tableInfo struct {
	dbfields []string
	values   string
	fields   string
	tname    string
	bclose   bool

	values_queue chan string
	dbase        *dataBaseInfo
	in_count     int
	insert_count int
	sync.RWMutex
}

const (
	destory_timeout_count int = 5
)

func newTable(tname string, data map[string]interface{}, dbase *dataBaseInfo) *tableInfo {
	t := &tableInfo{
		tname:        tname,
		values_queue: make(chan string, dbase.queue_len),
		dbase:        dbase,
	}
	for k, _ := range data {
		if k != "table_name" {
			t.dbfields = append(t.dbfields, k)
		}
	}
	t.fields = "(" + strings.Join(t.dbfields, ",") + ")"
	go t.run()
	return t
}

func (t *tableInfo) handle(data map[string]interface{}) error {
	t.RLock()
	if t.bclose {
		t.RUnlock()
		return fmt.Errorf("table closing. table name:%s", t.tname)
	}
	t.RUnlock()

	values := make([]string, 0, 30)
	for i := 0; i < len(t.dbfields); i++ {
		k := t.dbfields[i]
		switch vv := data[k].(type) {
		case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
			values = append(values, fmt.Sprintf("%d", vv))
		case float32, float64:
			values = append(values, fmt.Sprintf("%.0f", vv))
		case string:
			values = append(values, "\""+vv+"\"")
		default:
			return fmt.Errorf("table values type error, tname:%s, v:%+v k:%+v", t.tname, vv, k)
		}
	}
	t.in_count++
	tv := "(" + strings.Join(values, ",") + ")"
	t.values_queue <- tv

	return nil
}

func (t *tableInfo) run() {
	var count, timeoutCount int

	timeout := time.NewTicker(time.Second * 5)
	log.Info("table run tick time:%ds tname:%s", 5, t.tname)
	for {
		select {
		case v, ok := <-t.values_queue:
			if ok == false {
				t.insert_count += count
				t.insert()
				log.Info("table channel close and empty tname:%s in_count:%d insert_count:%d count:%d", t.tname, t.in_count, t.insert_count, count)
				goto exit
			}
			if t.values == "" {
				t.values = v
			} else {
				t.values += "," + v
			}
			count++
			if count >= t.dbase.save_count && t.insert() == nil {
				log.Debug("value insert success count:%d tname:%s", count, t.tname)
				t.insert_count += count
				timeoutCount = 0
				count = 0
			}
		case <-timeout.C:
			timeoutCount++
			if timeoutCount > destory_timeout_count { //如果没有数据进来，定时自我销毁
				timeout.Stop()
				timeoutCount = 0
				t.tclose()
				log.Info("time to destory goroute tname:%s", t.tname)
			} else if count > 0 {
				if t.insert() == nil { //定时存入mysql
					log.Debug("time  insert success count:%d tname:%s", count, t.tname)
					t.insert_count += count
					count = 0
				}
				timeoutCount = 0
			}
		}
	}
exit:
	t.dbase.Done()
	t.dbase.removeTable(t.tname)
}

func (t *tableInfo) insert() error {
	cmd := fmt.Sprintf("insert into %s %s values %s", t.tname, t.fields, t.values)
	_, err := t.dbase.db.Exec(cmd)
	if err != nil {
		log.Error("table insert err:%v cmd:%s", err, cmd)
		return err
	}
	t.values = ""
	return nil
}

func (t *tableInfo) tclose() {
	t.Lock()
	defer t.Unlock()

	t.bclose = true
	close(t.values_queue)
}
