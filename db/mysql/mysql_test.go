package mysql

import (
	"log"
	"testing"
	"time"

	"github.com/adamluo159/ydata/config"
)

type ydataTable struct {
	Amount    int64  `json:"amount"`
	Name      string `json:"name"`
	TableName string `json:"table_name"`
}

var (
	gdatabase IDataBaseInfo
)

func TestMain(m *testing.M) {
	cfg := &config.MysqlConfig{
		Addr:       "127.0.0.1",
		Database:   "test",
		BufferSize: 20,
		SaveCount:  10,
	}
	dbase, err := New(cfg)
	if err != nil {
		panic(err)
	}
	gdatabase = dbase

	m.Run()
}

func TestInsert(t *testing.T) {
	yd := &ydataTable{1, "lzy", "test_ydata"}
	byte_yd, _ := json.Marshal(yd)

	//插入进去一个
	err := gdatabase.Handle(byte_yd)
	if err != nil {
		t.Errorf("step 1 error :%v", err)
	}

	//插入10个
	yd.Amount = 2
	byte_yd, _ = json.Marshal(yd)
	for i := 0; i < 10; i++ {
		err := gdatabase.Handle(byte_yd)
		if err != nil {
			t.Errorf("step 2 error :%v", err)
		}
	}

	time.Sleep(time.Minute)

	//自动销毁后，再重新插入
	yd.Amount = 3
	byte_yd, _ = json.Marshal(yd)
	err = gdatabase.Handle(byte_yd)
	if err != nil {
		t.Errorf("step 2 error :%v", err)
	}

	time.Sleep(time.Second * 15)
}

func TestClose(t *testing.T) {
	gdatabase.Close()

	cfg := &config.MysqlConfig{
		Addr:       "127.0.0.1",
		Database:   "test",
		BufferSize: 100,
		SaveCount:  80,
	}
	dbase, err := New(cfg)
	if err != nil {
		panic(err)
	}

	go func() {
		yd := &ydataTable{4, "lzy", "test_ydata"}
		byte_yd, _ := json.Marshal(yd)

		for {
			err := dbase.Handle(byte_yd)
			if err != nil {
				log.Println(err)
				break
			}
			time.Sleep(time.Millisecond)
		}
	}()
	time.Sleep(time.Second * 10)
	dbase.Close()
	time.Sleep(time.Minute)
}
