package main

import (
	. "github.com/zhuyixiang/go-canal/events"
	. "github.com/zhuyixiang/go-canal/canal"
	. "github.com/zhuyixiang/go-canal/apply"
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

func testCanal() {
	config := &MysqlConfig{}
	config.Host = *testHost
	config.User = *testUser
	config.Port = 3306
	config.Password = *testPassword
	config.ServerID = 5004
	config.Pos = Position{}

	canal := NewCanal(config)

	taskPool, err := NewTaskPool("root:123456@tcp(172.16.20.146:3307)/information_schema", 1)
	if err != nil {

	}

	eventHandler := NewApplyDBEventHandler(taskPool)

	canal.SetEventHandler(eventHandler)

	//canal.SetEventHandler(&MyEventHandler{})
	canal.Start()

	//table, _ := canal.GetTable("test1", "t2")

	//log.Info(table.Name)

	time.Sleep(10 * time.Second)
}
