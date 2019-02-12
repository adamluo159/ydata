package nsq

import (
	"log"
	"sync/atomic"
	"testing"
	"time"

	nsq "github.com/nsqio/go-nsq"
)

var (
	cmgr *ConsumerMgr
	pd   *nsq.Producer
)

type Work struct {
}

func (w *Work) Handle(body []byte) error {
	return nil
}

func (w *Work) Close() {
	log.Println("close .....")
}

func GoPush(closeChan chan bool, topic string, count *uint64) {
	t := time.NewTicker(time.Microsecond * 5)
	for {
		select {
		case <-t.C:
			err := pd.MultiPublish(topic, [][]byte{[]byte("a"), []byte("a")})
			if err != nil {
				log.Println(err)
				break
			}
			atomic.AddUint64(count, 2)
		case <-closeChan:
			break
		}
	}
	t.Stop()
	pd.Stop()
}

func TestMain(m *testing.M) {
	lookupd_addr := []string{":4101"}
	cfgs := []*ConsumerCfg{
		&ConsumerCfg{"madaha", "name", &Work{}},
	}
	mg, err := NewConSumerMgr(lookupd_addr, cfgs, nsq.NewConfig())
	if err != nil {
		panic(err)
	}
	cmgr = mg

	p, err := nsq.NewProducer(":4200", nsq.NewConfig())
	if err != nil {
		panic(err)
	}
	pd = p
	m.Run()
}

func TestTopic(t *testing.T) {
	var test1_count, test2_count uint64

	c1 := make(chan bool, 1)
	go GoPush(c1, "test1", &test1_count)
	err := cmgr.AddConsumer(&ConsumerCfg{"test1", "1", &Work{}})
	if err != nil {
		panic(err)
	}

	c2 := make(chan bool, 1)
	go GoPush(c2, "test2", &test2_count)
	err = cmgr.AddConsumer(&ConsumerCfg{"test2", "1", &Work{}})
	if err != nil {
		panic(err)
	}

	time.Sleep(time.Second * 5)

	log.Println("test1 producer count:", atomic.LoadUint64(&test1_count))
	cmgr.RemoveConsumer("test1")
	c1 <- true

	time.Sleep(time.Second * 5)

	log.Println("test2 producer count:", atomic.LoadUint64(&test2_count))
	cmgr.RemoveConsumer("test2")
	c2 <- true

	cmgr.Close()

	err = cmgr.RemoveConsumer("madaha")
	if err != nil {
		log.Println(err)
	}
}

func TestClose(t *testing.T) {
	c := make(chan bool, 1)
	var count uint64
	go GoPush(c, "test2", &count)

	err := cmgr.AddConsumer(&ConsumerCfg{"test2", "1", &Work{}})
	if err != nil {
		panic(err)
	}
	time.Sleep(time.Second * 10)

	cmgr.Close()
	c <- true
	log.Println("consumer count:", cmgr.GetFinishedMessageCount("test2"))
	log.Println("producer count:", count)
}
