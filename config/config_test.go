package config

import (
	"log"
	"testing"
)

func TestConfig(t *testing.T) {
	cfg, err := NewConfig("cfg/nsql.cfg")
	if err != nil {
		log.Println(err)
		return
	}
	for _, v := range cfg.Nsqs {
		log.Printf("nsqs :%+v\n", v)
	}
	for _, v := range cfg.Mysqls {
		log.Printf("msqls:%+v\n", v)
	}
	log.Printf("loglevel:%+v\n", cfg.LogLevel)
}
