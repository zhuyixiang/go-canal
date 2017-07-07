package main

import (
	"fmt"
	"flag"
	. "github.com/zhuyixiang/go-canal/client"
	"github.com/ngaut/log"
)

var testHost = flag.String("host", "172.16.20.146", "MySQL server host")
var testPort = flag.Int("port", 3306, "MySQL server port")
var testUser = flag.String("user", "root", "MySQL user")
var testPassword = flag.String("pass", "123456", "MySQL password")
var testDB = flag.String("db", "test1", "MySQL test database")



func main() {
	var err error
	addr := fmt.Sprintf("%s:%d", *testHost, *testPort)
	conn, err := Connect(addr, *testUser, *testPassword, *testDB)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(conn.Sequence)
}
