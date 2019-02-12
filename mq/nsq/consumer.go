package nsq

import nsq "github.com/nsqio/go-nsq"

type Worker interface {
	Handle([]byte) error
	Close()
}

type ConsumerCfg struct {
	Topic   string
	Channel string
	worker  Worker
}

type yConsumer struct {
	consu *nsq.Consumer
	cfg   *ConsumerCfg
}
