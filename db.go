package dseries

import (
	"fmt"
	"github.com/fpawel/comm/modbus"
	"github.com/fpawel/gohelp"
	"github.com/jmoiron/sqlx"
	"sync"
	"time"
)

//go:generate go run github.com/fpawel/goutils/dbutils/sqlstr/...

func Open(fileName string) {
	db = gohelp.OpenSqliteDBx(fileName)
	db.MustExec(SQLCreate)
}

func Close () error{
	return db.Close()
}

func CreateNewBucket(name string) {
	db.MustExec(`DELETE FROM bucket WHERE created_at = updated_at; INSERT INTO bucket (name) VALUES (?)`, name)
}

// AddPoint - добавить новую точку в кеш.
func AddPoint(addr modbus.Addr, v modbus.Var, value float64) {
	muPoints.Lock()
	defer muPoints.Unlock()
	currentPoints = append(currentPoints, point{
		StoredAt: time.Now(),
		Addr:     addr,
		Var:      v,
		Value:    value,
	})
	if time.Since(currentPoints[0].StoredAt) > time.Minute {
		save()
	}
}

// Save - сохранить точки из кеша, очистить кеш. Можно вызывать кокурентно.
func Save() {
	muPoints.Lock()
	defer muPoints.Unlock()
	save()
}


func lastBucket() (buck bucket) {
	if err := db.Get(&buck, `SELECT bucket_id, name, created_at, updated_at FROM last_bucket`); err != nil {
		panic(err)
	}
	return
}

func save() {
	if len(currentPoints) == 0 {
		return
	}
	queryInsertPoints := queryInsertPoints()
	currentPoints = nil
	go db.MustExec(queryInsertPoints)
}

func queryInsertPoints() string {
	queryStr := `INSERT INTO series(bucket_id, Addr, var, Value, stored_at)  VALUES `
	for i, a := range currentPoints {

		s := fmt.Sprintf("(%d, %d, %d, %v,", lastBucket().BucketID, a.Addr, a.Var, a.Value) +
			"julianday(STRFTIME('%Y-%m-%d %H:%M:%f','" +
			a.StoredAt.Format("2006-01-02 15:04:05.000") + "')))"

		if i < len(currentPoints)-1 {
			s += ", "
		}
		queryStr += s
	}
	return queryStr
}


var (
	db            *sqlx.DB
	currentPoints []point
	muPoints      sync.Mutex
)


