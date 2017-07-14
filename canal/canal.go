package canal

import (
	"github.com/zhuyixiang/go-canal/client"
	"golang.org/x/net/context"
	. "github.com/zhuyixiang/go-canal/events"
)

type Canal struct {
	cfg    *MysqlConfig
	syncer *BinlogSyncer
	conn   *client.Conn
	ctx    context.Context
	cancel context.CancelFunc
}

func NewCanal(cfg *MysqlConfig) {
	c := new(Canal)
	c.cfg = cfg
	c.syncer = NewBinlogSyncer(cfg)
	c.ctx, c.cancel = context.WithCancel(context.Background())
}