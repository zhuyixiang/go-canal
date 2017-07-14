package canal

import (
	"github.com/zhuyixiang/go-canal/client"
	"golang.org/x/net/context"
	. "github.com/zhuyixiang/go-canal/events"
	"github.com/juju/errors"
	"github.com/siddontang/go-mysql/schema"
	"fmt"
)

type Canal struct {
	cfg          *MysqlConfig

	syncer       *BinlogSyncer
	conn         *client.Conn

	ctx          context.Context
	cancel       context.CancelFunc

	tables       map[string]*Table
	eventHandler EventHandler

	curPos       Position
}

func NewCanal(cfg *MysqlConfig) {
	c := new(Canal)
	c.cfg = cfg
	c.curPos = cfg.Pos
	c.syncer = NewBinlogSyncer(cfg)
	c.ctx, c.cancel = context.WithCancel(context.Background())
}

func (c *Canal) SetEventHandler(h EventHandler) {
	c.eventHandler = h
}

func (c *Canal) GetTable(db string, table string) (*schema.Table, error) {
	key := fmt.Sprintf("%s.%s", db, table)
	t, ok := c.tables[key]

	if ok {
		return t, nil
	}

	t, err := schema.NewTable(c, db, table)
	if err != nil {
		// check table not exists
		if ok, err1 := schema.IsTableExist(c, db, table); err1 == nil && !ok {
			return nil, schema.ErrTableNotExist
		}

		return nil, errors.Trace(err)
	}

	c.tables[key] = t

	return t, nil
}
func (c *Canal) ClearTableCache(db []byte, table []byte) {
	key := fmt.Sprintf("%s.%s", db, table)
	delete(c.tables, key)
}


// Execute a SQL
func (c *Canal) Execute(cmd string, args ...interface{}) (rr *Result, err error) {

	retryNum := 3
	for i := 0; i < retryNum; i++ {
		if c.conn == nil {
			c.conn, err = client.Connect(c.cfg.Host + ":" + c.cfg.Port, c.cfg.User, c.cfg.Password, "")
			if err != nil {
				return nil, errors.Trace(err)
			}
		}

		rr, err = c.conn.Execute(cmd, args...)
		if err != nil && !ErrorEqual(err, ErrBadConn) {
			return
		} else if ErrorEqual(err, ErrBadConn) {
			c.conn.Close()
			c.conn = nil
			continue
		} else {
			return
		}
	}
	return
}