package events

import (

)
import "github.com/siddontang/go-mysql/replication"

type EventHandler interface {
	OnRotate(roateEvent *RotateEvent) error
	OnDDL(nextPos Position, queryEvent *replication.QueryEvent) error
	OnRow(e *RowsEvent) error
	OnXID(nextPos Position) error
	String() string
}

type DummyEventHandler struct {
}

func (h *DummyEventHandler) OnRotate(*replication.RotateEvent) error { return nil }
func (h *DummyEventHandler) OnDDL(Position, *replication.QueryEvent) error {
	return nil
}
func (h *DummyEventHandler) OnRow(*RowsEvent) error     { return nil }
func (h *DummyEventHandler) OnXID(Position) error { return nil }
func (h *DummyEventHandler) String() string             { return "DummyEventHandler" }


