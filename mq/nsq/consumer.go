package nsq

import (
	"github.com/adamluo159/ydata/config"
	nsq "github.com/nsqio/go-nsq"
)

type Worker interface {
	Handle([]byte) error
	Close()
}

type yConsumer struct {
	consu  *nsq.Consumer
	cfg    *config.ConsumerConfig
	worker Worker
}
