package main

import (
	"flag"
	"os"
	"os/signal"

	"github.com/adamluo159/ydata/config"
	"github.com/adamluo159/ydata/db/mysql"
	"github.com/adamluo159/ydata/mq/nsq"
)

func main() {
	cfg_path := flag.String("cfg_path", "../../config/cfg/nsql.cfg", "nsql config path")
	flag.Parse()

	cfg, err := config.NewConfig(*cfg_path)
	if err != nil {
		panic(err)
	}

	m := nsq.NewConSumerMgr(cfg.ClientConfig, cfg.LookupdAddr)
	for _, v := range cfg.Nsqs {
		worker, err := mysql.New(v.Mysql)
		if err != nil {
			panic(err)
		}
		m.AddConsumer(v.Consumer, worker)
	}
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, os.Kill)
	<-c
}
