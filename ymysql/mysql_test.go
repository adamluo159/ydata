package ymysql

import (
	"log"
	"testing"
	"time"
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
	dbase, err := New("", "", "127.0.0.1", "test", 20, 10)
	if err != nil {
		panic(err)
	}
	gdatabase = dbase

	m.Run()
}

func TestHandle(t *testing.T) {
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

	time.Sleep(time.Minute)
}

func TestClose(t *testing.T) {
	gdatabase.Close()

	dbase, err := New("", "", "127.0.0.1", "test", 30, 1)
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
	dbase.Close()
	time.Sleep(time.Minute)
}
