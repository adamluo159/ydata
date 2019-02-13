package config

import (
	"io/ioutil"

	"github.com/BurntSushi/toml"
	"github.com/nsqio/go-nsq"
)

type NsqConfig struct {
	Addr        []string
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
	Topic      string
}

type Config struct {
	Nsqs         []*NsqConfig
	Mysqls       []*MysqlConfig
	LogLevel     string
	ClientConfig *nsq.Config
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
