package main

import (
	"fmt"
	. "github.com/zhuyixiang/go-canal/events"
	. "github.com/zhuyixiang/go-canal/canal"
	"time"
)

func testDumpLog() {
	config := &MysqlConfig{}
	config.Host = *testHost
	config.User = *testUser
	config.Port = 3306
	config.Password = *testPassword
	config.ServerID = 5004
	//config.Charset = "utf-8"


	syncer := NewBinlogSyncer(config)

	position := Position{}
	position.Pos = 0
	//position.Name = "mysql-bin.000038"

	syncer.StartSync(position)

	go printEvent(syncer)

	time.Sleep(10 * time.Second)
}

func printEvent(syncer *BinlogSyncer) {
	for {
		if (syncer.IsClosed()) {
			break
		}
		binlogEvent, err := syncer.GetEvent()
		if (err != nil) {
			fmt.Println(err)
			break
		}

		fmt.Print("[")
		for _, byte := range binlogEvent.RawData {
			fmt.Printf("%x ", byte)
		}
		fmt.Println("]");

	}
}