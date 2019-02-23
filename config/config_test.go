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
		log.Printf("nsqs :%+v mysql:%+v consumer:%+v \n ", v, v.Mysql, v.Consumer)
	}
	log.Printf("loglevel:%+v\n", cfg.LogLevel)
	log.Println("lookupds:", cfg.LookupdAddr)
}
