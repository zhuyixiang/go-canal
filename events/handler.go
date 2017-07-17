package events

type EventHandler interface {
	OnRotate(roateEvent *RotateEvent) error
	OnDDL(nextPos Position, queryEvent *QueryEvent) error
	OnRow(e *RowsEvent) error
	OnXID(nextPos Position) error
	String() string
}

type DummyEventHandler struct {
}

func (h *DummyEventHandler) OnRotate(*RotateEvent) error { return nil }
func (h *DummyEventHandler) OnDDL(Position, *QueryEvent) error {
	return nil
}
func (h *DummyEventHandler) OnRow(*RowsEvent) error     { return nil }
func (h *DummyEventHandler) OnXID(Position) error { return nil }
func (h *DummyEventHandler) String() string             { return "DummyEventHandler" }


