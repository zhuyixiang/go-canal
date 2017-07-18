package apply

import (
	"database/sql"
	"sync"
	"hash/fnv"
	"fmt"
	. "github.com/zhuyixiang/go-canal/events"
)

type TaskPool struct {
	dbMap        map[int]*sql.DB
	connStr      string
	size         int
	eventChanMap map[int](chan *WrapEvent)
}

type WrapEvent struct {
	eventType int
	mutex     *sync.Mutex
	event     interface{}
	done      int
}

func NewWrapEvent(eventType int, event interface{}) (*WrapEvent) {
	wrapEvent := new(WrapEvent)
	if eventType == 2 {
		wrapEvent.mutex = new(sync.Mutex)
		wrapEvent.done = 0
	}

	wrapEvent.eventType = eventType
	wrapEvent.event = event
	return wrapEvent
}

func NewTaskPool(connStr string, size int) (*TaskPool, error) {
	taskpool := new(TaskPool)
	taskpool.connStr = connStr
	taskpool.size = size

	taskpool.dbMap = make(map[int]*sql.DB, size)
	taskpool.eventChanMap = make(map[int](chan *WrapEvent), size)

	err := taskpool.initDbMap()
	if err != nil {
		return nil, err
	}
	return taskpool, nil
}

func (taskPool *TaskPool) initDbMap() (err error) {
	for i := 0; i < taskPool.size; i++ {
		db, err := sql.Open("mysql", taskPool.connStr)
		if err != nil {
			return err
		}
		taskPool.dbMap[i] = db
		ch := make(chan *WrapEvent)
		taskPool.eventChanMap[i] = ch

	}
	return nil
}

func (taskPool *TaskPool) processEvent(ch <-chan WrapEvent, db sql.DB) (err error) {
	for {
		event := <-ch
		if (event.eventType == 1) {
			origin := event.event.(*RowsEvent)
			if(origin.Action == "INSERT"){
				insertSql := "insert into " + origin.Table.Schema + "." + origin.Table.Name
				insertSql += " ("
				valuesSql := " values( "
				for index, col := range origin.Table.Columns{
					if(index == len(origin.Table.Columns) -1 ){
						insertSql += " " + col.Name + ")"
						valuesSql += "?)"
					}else {
						insertSql += " " + col.Name + ","
						valuesSql += "?, "
					}
				}
				insertSql += valuesSql;
				for _, value := range origin.Rows{
					insertStmt, err := db.Prepare(insertSql);
					if err != nil {
						fmt.Println(err)
						break
					}
					insertStmt.Exec(value)
				}
			}
		} else {
			event.mutex.Lock()
			if event.done == 1 {
				continue
			}
			sql := event.event.(string)
			_, err := db.Exec(sql)
			if err != nil {

			}
			event.done = 1
			defer event.mutex.Unlock()
		}
		return nil
	}

}

func hash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

func (taskPool *TaskPool) calcEventSlot(rowEvent *RowsEvent) (int) {
	slot := 0
	if (rowEvent.Action == "INSERT") {
		pkString := ""
		for index := range rowEvent.Table.PKColumns {
			pkString += rowEvent.Rows[0][index].(string);
		}
		slot = int(hash(pkString)) % taskPool.size
	}
	return slot
}

func (taskPool *TaskPool) PutEvent(rowEvent *RowsEvent) (err error) {
	chanSlot := taskPool.calcEventSlot(rowEvent)
	wrapEvent := NewWrapEvent(1, rowEvent)
	taskPool.eventChanMap[chanSlot] <- wrapEvent

	return nil
}

func (taskPool *TaskPool) ExecQuery(sql string) (err error) {
	for _, channal := range taskPool.eventChanMap {
		wrapEvent := NewWrapEvent(1, sql)
		channal <- wrapEvent
	}
	return nil
}
