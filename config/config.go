package config

import (
	"io/ioutil"

	"github.com/BurntSushi/toml"
	nsq "github.com/nsqio/go-nsq"
)

type NsqConfig struct {
	Consumer *ConsumerConfig
	Mysql    *MysqlConfig
}

type ConsumerConfig struct {
	Topic       string
	Channel     string
	MaxInFlight int
}

type MysqlConfig struct {
	Addr       string
	Username   string
	Password   string
	Database   string
	BufferSize int
	SaveCount  int
}

type Config struct {
	Nsqs         []*NsqConfig
	LookupdAddr  []string
	ClientConfig *nsq.Config
	LogLevel     string
}

func NewConfig(path string) (*Config, error) {
	b, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}

	config := &Config{}
	err = toml.Unmarshal(b, config)
	if err != nil {
		return nil, err
	}
	config.ClientConfig = nsq.NewConfig()
	return config, nil

}
