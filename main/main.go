package main

import (
	"fmt"
	"flag"
	"github.com/zhuyixiang/go-canal/client"
	"github.com/ngaut/log"
	"golang.org/x/net/context"
	"time"
)

var testHost = flag.String("host", "172.16.20.146", "MySQL server host")
var testPort = flag.Int("port", 3306, "MySQL server port")
var testUser = flag.String("user", "root", "MySQL user")
var testPassword = flag.String("pass", "123456", "MySQL password")
var testDB = flag.String("db", "test1", "MySQL test database")

func main() {
	//testConnect()

	//testDumpLog()

	//testContext();

	//testCanal()

	testMysqlDriver()

	time.Sleep(time.Second * 5)
}

func testConnect() {
	var err error
	addr := fmt.Sprintf("%s:%d", *testHost, *testPort)
	conn, err := client.Connect(addr, *testUser, *testPassword, *testDB)
	//defer conn.Close()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(conn.Sequence)

	result, _ := conn.Execute("select * from t1;");

	//result, _ = conn.Execute("select * from t1;");

	fmt.Println(result.RowDatas)
}

func testContext() {
	ctx, cancel := context.WithCancel(context.Background())
	go getCancelEvent(ctx)
	go getCancelEvent(ctx)
	cancel()
	go getCancelEvent(ctx)
	go getCancelEvent(ctx)
}

func getCancelEvent(ctx context.Context) {
	<-ctx.Done()
	fmt.Println("done")
}