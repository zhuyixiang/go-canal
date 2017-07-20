package apply

import (
	"database/sql"
	"sync"
	"hash/fnv"
	"fmt"
	. "github.com/zhuyixiang/go-canal/events"
	//"reflect"
	//"strconv"
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
		ch := make(chan *WrapEvent, 1024)
		taskPool.eventChanMap[i] = ch
		go taskPool.processEvent(ch, db)
	}
	return nil
}

func (taskPool *TaskPool) processEvent(ch <-chan *WrapEvent, db *sql.DB) (err error) {
	defer func() {
		if e := recover(); e != nil {
			fmt.Errorf("Err: %v\n Stack: %s", e, Pstack())
		}
	}()
	for {
		event := <-ch
		if (event.eventType == 1) {
			origin := event.event.(*RowsEvent)
			if (origin.Action == "insert") {
				insertSql := "insert into " + origin.Table.Schema + "." + origin.Table.Name
				insertSql += " ("
				valuesSql := " values( "
				for index, col := range origin.Table.Columns {
					if (index == len(origin.Table.Columns) - 1 ) {
						insertSql += " `" + col.Name + "`)"
						valuesSql += "?)"
					} else {
						insertSql += " `" + col.Name + "`,"
						valuesSql += "?, "
					}
				}
				insertSql += valuesSql
				fmt.Println(insertSql)
				tx, err := db.Begin()
				if err != nil {
					fmt.Println(err)
				}
				for _, value := range origin.Rows {
					insertStmt, err := db.Prepare(insertSql);
					if err != nil {
						fmt.Println(err)

					}
					_, err = insertStmt.Exec(value...)
					if err != nil {
						fmt.Println(err)
					}
				}
				err = tx.Commit()
				if err != nil {
					fmt.Println(err)

				}
			} else if (origin.Action == "update") {
				updateSql := "update " + origin.Table.Schema + "." + origin.Table.Name
				updateSql += " set "
				whereSql := " where 1=1 "
				for index, col := range origin.Table.Columns {
					if (index == len(origin.Table.Columns) - 1 ) {
						updateSql += "`" + col.Name + "` = ?"
						if (containsInPk(origin.Table.PKColumns, index)) {
							whereSql += "and `" + col.Name + "` = ? "
						}
					} else {
						updateSql += "`" + col.Name + "` = ?, "
						if (containsInPk(origin.Table.PKColumns, index)) {
							whereSql += "and `" + col.Name + "` = ? "
						}
					}
				}
				updateSql += whereSql
				fmt.Println(updateSql)
				tx, err := db.Begin()
				if err != nil {
					fmt.Println(err)
				}
				sqlArgs := make([]interface{}, 0, len(origin.Table.PKColumns) * 2)
				sqlArgs = append(sqlArgs, origin.Rows[1]...)
				for index, value := range origin.Rows[0] {
					if (containsInPk(origin.Table.PKColumns, index)) {
						sqlArgs = append(sqlArgs, value);
					}
				}

				insertStmt, err := db.Prepare(updateSql);
				if err != nil {
					fmt.Println(err)

				}
				_, err = insertStmt.Exec(sqlArgs...)
				if err != nil {
					fmt.Println(err)
				}
				err = tx.Commit()
				if err != nil {
					fmt.Println(err)

				}

			} else if (origin.Action == "delete") {
				deleteSql := "delete from  " + origin.Table.Schema + "." + origin.Table.Name
				deleteSql += " where 1=1 "
				for index, col := range origin.Table.Columns {
					if (index == len(origin.Table.Columns) - 1 ) {
						if (containsInPk(origin.Table.PKColumns, index)) {
							deleteSql += "and `" + col.Name + "` = ? "
						}
					} else {
						if (containsInPk(origin.Table.PKColumns, index)) {
							deleteSql += "and `" + col.Name + "` = ? "
						}
					}
				}
				fmt.Println(deleteSql)
				tx, err := db.Begin()
				if err != nil {
					fmt.Println(err)
				}
				sqlArgs := make([]interface{}, 0, len(origin.Table.PKColumns))
				for index, value := range origin.Rows[0] {
					if (containsInPk(origin.Table.PKColumns, index)) {
						sqlArgs = append(sqlArgs, value);
					}
				}

				insertStmt, err := db.Prepare(deleteSql);
				if err != nil {
					fmt.Println(err)

				}
				_, err = insertStmt.Exec(sqlArgs...)
				if err != nil {
					fmt.Println(err)
				}
				err = tx.Commit()
				if err != nil {
					fmt.Println(err)

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
	}
	return nil
}

func containsInPk(array []int, value int) (bool) {
	isExists := false
	for _, cValue := range array {
		if (cValue == value) {
			isExists = true
		}
	}
	return isExists
}

func hash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

func (taskPool *TaskPool) calcEventSlot(rowEvent *RowsEvent) (int) {
	slot := 0
	//if (rowEvent.Action == "insert") {
	//	pkString := ""
	//	for index := range rowEvent.Table.PKColumns {
	//		typeName := reflect.TypeOf(rowEvent.Rows[0][index]).Name()
	//		if typeName == "int32" || typeName == "int" {
	//			p := rowEvent.Rows[0][index].(int32)
	//			pkString += strconv.Itoa(int(p))
	//		} else if typeName == "string" {
	//			pkString += rowEvent.Rows[0][index].(string)
	//		} else {
	//			fmt.Errorf("calcEventSlot", "type error")
	//		}
	//	}
	//	slot = int(hash(pkString)) % taskPool.size
	//}
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
