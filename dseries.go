package dseries

import (
	"database/sql"
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

func Close() error {
	return db.Close()
}

func CreateNewBucket(name string) {
	db.MustExec(`DELETE FROM bucket WHERE created_at = updated_at; INSERT INTO bucket (name) VALUES (?)`, name)
}

// AddPoint - добавить новую точку в кеш.
func AddPoint(addr modbus.Addr, v modbus.Var, value float64) {
	muPoints.Lock()
	defer muPoints.Unlock()
	pt := point{
		StoredAt: time.Now(),
		Addr:     addr,
		Var:      v,
		Value:    value,
	}
	currentPoints = append(currentPoints, pt)
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

func UpdatedAt() time.Time {
	muPoints.Lock()
	defer muPoints.Unlock()
	if len(currentPoints) > 0 {
		return currentPoints[len(currentPoints)-1].StoredAt
	}
	if b, f := lastBucket(); f {
		return b.UpdatedAt
	}
	return time.Unix(0, 0)
}

func mustLastBucket() bucket {
	b, f := lastBucket()
	if !f {
		panic("no buckets")
	}
	return b
}

func lastBucket() (bucket, bool) {
	var buck bucket
	err := db.Get(&buck, `SELECT bucket_id, name, created_at, updated_at FROM last_bucket`)
	if err == nil {
		return buck, true
	}
	if err != sql.ErrNoRows {
		panic(err)
	}
	return buck, false
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

		s := fmt.Sprintf("(%d, %d, %d, %v,", mustLastBucket().BucketID, a.Addr, a.Var, a.Value) +
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
