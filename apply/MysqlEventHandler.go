package apply

import (
	. "github.com/zhuyixiang/go-canal/events"
	"github.com/ngaut/log"
)

type ApplyDBEventHandler struct{
	DummyEventHandler
	taskPool *TaskPool
}

func NewApplyDBEventHandler(taskPool *TaskPool)(*ApplyDBEventHandler){
	dbEventHandler := new(ApplyDBEventHandler)
	dbEventHandler.taskPool = taskPool
	return dbEventHandler
}

func (h *ApplyDBEventHandler) OnRow(e *RowsEvent) error {
	log.Infof("%s %v %s\n", e.Action, e.Rows, e.Table)
	h.taskPool.PutEvent(e)
	return nil
}

func (h *ApplyDBEventHandler) OnDDL(nextPos Position, queryEvent *QueryEvent) error {
	log.Infof("%s \n", queryEvent.Query)
	h.taskPool.ExecQuery(string(queryEvent.Query))
	return nil
}
