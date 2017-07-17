package main

import (
	. "github.com/zhuyixiang/go-canal/events"
	. "github.com/zhuyixiang/go-canal/canal"
	"github.com/ngaut/log"
	"time"
)

type MyEventHandler struct {
	DummyEventHandler
}

func (h *MyEventHandler) OnRow(e *RowsEvent) error {
	log.Infof("%s %v %s\n", e.Action, e.Rows, e.Table)
	return nil
}

func (h *MyEventHandler) OnDDL(nextPos Position, queryEvent *QueryEvent) error {
	log.Infof("%s \n", queryEvent.Query)
	return nil
}

func testCanal(){
	config := &MysqlConfig{}
	config.Host = *testHost
	config.User = *testUser
	config.Port = 3306
	config.Password = *testPassword
	config.ServerID = 5004
	config.Pos = Position{}


	canal := NewCanal(config)

	canal.SetEventHandler(&MyEventHandler{})


	canal.Start()

	time.Sleep(10 * time.Second)
}
