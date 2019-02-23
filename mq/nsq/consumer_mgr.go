package nsq

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/adamluo159/ydata/config"
	"github.com/adamluo159/ydata/log"
	nsq "github.com/nsqio/go-nsq"
)

type ConsumerMgr struct {
	consumer_map sync.Map
	nsq_cfg      *nsq.Config

	lookupd_addrs []string
	close_chan    chan bool
	close_flag    int32
}

func NewConSumerMgr(cfg *nsq.Config, lookupd_addrs []string) *ConsumerMgr {
	return &ConsumerMgr{
		nsq_cfg:       cfg,
		lookupd_addrs: lookupd_addrs,
		close_chan:    make(chan bool, 1),
	}
}

func (m *ConsumerMgr) Close() {
	atomic.AddInt32(&m.close_flag, 1)
	m.close_chan <- true
	m.consumer_map.Range(func(key, value interface{}) bool {
		v := value.(*yConsumer)
		v.consu.Stop()
		<-v.consu.StopChan
		v.worker.Close()
		return true
	})
}

func (m *ConsumerMgr) AddConsumer(cfg *config.ConsumerConfig, worker Worker) error {
	if atomic.LoadInt32(&m.close_flag) == 1 {
		return fmt.Errorf("consumer mgr is closing.")
	}

	consu, err := nsq.NewConsumer(cfg.Topic, cfg.Channel, m.nsq_cfg)
	if err != nil {
		return err
	}

	consu.AddHandler(nsq.HandlerFunc(func(m *nsq.Message) error {
		return worker.Handle(m.Body)
	}))

	err = consu.ConnectToNSQLookupds(m.lookupd_addrs)
	if err != nil {
		consu.Stop()
		<-consu.StopChan
		return err
	}

	_, loaded := m.consumer_map.LoadOrStore(cfg.Topic, &yConsumer{consu, cfg, worker})
	if loaded {
		consu.Stop()
		<-consu.StopChan
		return fmt.Errorf("topic exist")
	}
	return nil
}

func (m *ConsumerMgr) RemoveConsumer(topic string) error {
	if atomic.LoadInt32(&m.close_flag) == 1 {
		return fmt.Errorf("consumer mgr is closing.")
	}

	v, ok := m.consumer_map.Load(topic)
	if !ok {
		return fmt.Errorf("cant find topic:%s", topic)
	}
	vv := v.(*yConsumer)
	if vv == nil {
		return fmt.Errorf("cant find topic:%s", topic)
	}
	log.Info("topic:%s remove finished message count:%d", topic, vv.consu.Stats().MessagesFinished)

	vv.consu.Stop()
	<-vv.consu.StopChan

	vv.worker.Close()

	<-vv.consu.StopChan
	m.consumer_map.Delete(topic)
	return nil
}

func (m *ConsumerMgr) checkloop() {
	tick := time.NewTicker(time.Hour)
	for {
		select {
		case <-tick.C:
			if atomic.LoadInt32(&m.close_flag) == 1 {
				break
			}
			m.checkValidTopic()
		case <-m.close_chan:
			break
		}
	}
	tick.Stop()
}

type lookupTopics struct {
	Topics []string `json:"topics"`
}

func (m *ConsumerMgr) checkValidTopic() {
	ts := &lookupTopics{}
	err := ApiRequestNegotiateV1("GET", "/topics", nil, ts)
	if err != nil {
		log.Error("checkValidTopic %v", err)
		return
	}

	invalid_topics := make([]string, 0)
	m.consumer_map.Range(func(key, value interface{}) bool {
		found := false
		topic := key.(string)
		for _, v := range ts.Topics {
			if v == topic {
				found = true
				break
			}
		}
		if !found {
			invalid_topics = append(invalid_topics, topic)
		}
		return true
	})

	for _, v := range invalid_topics {
		value, ok := m.consumer_map.Load(v)
		if ok {
			vv := value.(*yConsumer)
			if vv != nil {
				m.consumer_map.Delete(v)

				vv.consu.Stop()
				<-vv.consu.StopChan
			}
		}
	}
}

func (m *ConsumerMgr) AddLookupdAddr(lookupd_addr string) error {
	if atomic.LoadInt32(&m.close_flag) == 1 {
		return fmt.Errorf("consumer mgr is closing.")
	}

	for _, v := range m.lookupd_addrs {
		if v == lookupd_addr {
			return fmt.Errorf("already exists ")
		}
	}

	m.consumer_map.Range(func(key, value interface{}) bool {
		v := value.(*yConsumer)
		if v != nil {
			err := v.consu.ConnectToNSQLookupd(lookupd_addr)
			if err != nil {
				log.Error("AddLookupdAddr %v", err)
			}
		}
		return true
	})
	return nil
}

func (m *ConsumerMgr) GetFinishedMessageCount(topic string) uint64 {
	v, ok := m.consumer_map.Load(topic)
	if !ok {
		return 0
	}
	vv := v.(*yConsumer)
	if vv == nil {
		return 0
	}
	stat := vv.consu.Stats()
	return stat.MessagesFinished
}
