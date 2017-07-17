package canal

import (
	"regexp"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	. "github.com/zhuyixiang/go-canal/events"
)

var (
	expAlterTable = regexp.MustCompile("(?i)^ALTER\\sTABLE\\s.*?`{0,1}(.*?)`{0,1}\\.{0,1}`{0,1}([^`\\.]+?)`{0,1}\\s.*")
)

func (c *Canal) startSyncBinlog(pos Position) error {
	log.Infof("start sync binlog at %v", pos)

	err := c.syncer.StartSync(pos)
	if err != nil {
		return errors.Errorf("start sync replication at %v error %v", pos, err)
	}

	for {
		ev, err := c.syncer.GetEvent()

		if err != nil {
			return errors.Trace(err)
		}

		curPos := pos.Pos
		//next binlog pos
		pos.Pos = ev.Header.LogPos

		// We only save position with RotateEvent and XIDEvent.
		// For RowsEvent, we can't save the position until meeting XIDEvent
		// which tells the whole transaction is over.
		// TODO: If we meet any DDL query, we must save too.
		switch e := ev.Event.(type) {
		case *RotateEvent:
			pos.Name = string(e.NextLogName)
			pos.Pos = uint32(e.Position)
			log.Infof("rotate binlog to %s", pos)

			if err = c.eventHandler.OnRotate(e); err != nil {
				return errors.Trace(err)
			}
		case *RowLogEvent:
			// we only focus row based event
			err = c.handleRowsEvent(ev)
			if err != nil && errors.Cause(err) != ErrTableNotExist {
				// We can ignore table not exist error
				log.Errorf("handle rows event at (%s, %d) error %v", pos.Name, curPos, err)
				return errors.Trace(err)
			}
			continue
		case *XIDEvent:
			// try to save the position later
			if err := c.eventHandler.OnXID(pos); err != nil {
				return errors.Trace(err)
			}
		case *QueryEvent:
			// handle alert table query
			if mb := expAlterTable.FindSubmatch(e.Query); mb != nil {
				if len(mb[1]) == 0 {
					mb[1] = e.Schema
				}
				c.ClearTableCache(mb[1], mb[2])
				log.Infof("table structure changed, clear table cache: %s.%s\n", mb[1], mb[2])
				if err = c.eventHandler.OnDDL(pos, e); err != nil {
					return errors.Trace(err)
				}
			} else {
				// skip others
				continue
			}
		default:
			continue
		}

		c.curPos = pos
	}

	return nil
}

func (c *Canal) handleRowsEvent(e *BinlogEvent) error {
	ev := e.Event.(*RowLogEvent)

	// Caveat: table may be altered at runtime.
	schema := string(ev.Table.Schema)
	table := string(ev.Table.Table)

	t, err := c.GetTable(schema, table)
	if err != nil {
		return errors.Trace(err)
	}
	var action string
	switch e.Header.EventType {
	case WRITE_ROWS_EVENTv1, WRITE_ROWS_EVENTv2:
		action = InsertAction
	case DELETE_ROWS_EVENTv1, DELETE_ROWS_EVENTv2:
		action = DeleteAction
	case UPDATE_ROWS_EVENTv1, UPDATE_ROWS_EVENTv2:
		action = UpdateAction
	default:
		return errors.Errorf("%s not supported now", e.Header.EventType)
	}
	events := NewRowsEvent(t, action, ev.Rows)
	return c.eventHandler.OnRow(events)
}

func (c *Canal) WaitUntilPos(pos Position, timeout time.Duration) error {
	timer := time.NewTimer(timeout)
	for {
		select {
		case <-timer.C:
			return errors.Errorf("wait position %v too long > %s", pos, timeout)
		default:
			curPos := c.curPos
			if curPos.Compare(pos) >= 0 {
				return nil
			} else {
				log.Debugf("master pos is %v, wait catching %v", curPos, pos)
				time.Sleep(100 * time.Millisecond)
			}
		}
	}

	return nil
}

func (c *Canal) CatchMasterPos(timeout time.Duration) error {
	rr, err := c.Execute("SHOW MASTER STATUS")
	if err != nil {
		return errors.Trace(err)
	}

	name, _ := rr.GetString(0, 0)
	pos, _ := rr.GetInt(0, 1)

	return c.WaitUntilPos(Position{name, uint32(pos)}, timeout)
}
