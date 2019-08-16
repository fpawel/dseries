package dseries

import (
	"encoding/binary"
	"github.com/fpawel/comm/modbus"
	"io"
	"net/http"
	"strconv"
)

type ChartsSvc struct{}

type YearMonth struct {
	Year  int `db:"year"`
	Month int `db:"month"`
}

func (_ *ChartsSvc) YearsMonths(_ struct{}, r *[]YearMonth) error {
	if err := db.Select(r, `SELECT DISTINCT year, month FROM bucket_time ORDER BY year DESC, month DESC`); err != nil {
		panic(err)
	}
	return nil
}

type ChartsBucket struct {
	Day      int    `db:"day"`
	Hour     int    `db:"hour"`
	Minute   int    `db:"minute"`
	BucketID int64  `db:"bucket_id"`
	Name     string `db:"name"`
	Last     bool   `db:"last"`
}

func (_ *ChartsSvc) BucketsOfYearMonth(x YearMonth, r *[]ChartsBucket) error {
	if err := db.Select(r, `
SELECT day, hour, minute, bucket_id, name, bucket_id = (SELECT bucket_id FROM last_bucket) AS last
FROM bucket_time
WHERE year = ?
  AND month = ?
ORDER BY created_at`, x.Year, x.Month); err != nil {
		panic(err)
	}
	return nil
}

func (_ *ChartsSvc) DeletePoints(r deletePointsRequest, rowsAffected *int64) error {

	lastBuckID := lastBucket().BucketID

	if r.BucketID == 0 {
		r.BucketID = lastBuckID
	}
	if r.BucketID == lastBuckID {
		muPoints.Lock()
		n := 0
		for _, x := range currentPoints {
			f := x.Addr == x.Addr && x.Var == x.Var &&
				x.StoredAt.After(r.TimeMinimum.Time()) && x.StoredAt.Before(r.TimeMaximum.Time()) &&
				x.Value >= r.ValueMinimum &&
				x.Value <= r.ValueMaximum
			if f {
				currentPoints[n] = x
				n++
			}
		}
		currentPoints = currentPoints[:n]
		muPoints.Unlock()
	}

	const timeFormat = "2006-01-02 15:04:05.000"
	var err error
	*rowsAffected, err = db.MustExec(
		`
DELETE FROM series 
WHERE bucket_id = ? AND 
      addr = ? AND 
      var = ? AND  
      value >= ? AND 
      value <= ? AND 
      stored_at >= julianday(?) AND 
      stored_at <= julianday(?);`, r.BucketID, r.Addr, r.Var,
		r.ValueMinimum, r.ValueMaximum,
		r.TimeMinimum.Time().Format(timeFormat),
		r.TimeMaximum.Time().Format(timeFormat)).RowsAffected()
	if err != nil {
		panic(err)
	}
	return nil
}

func HandleRequestChart(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(200)
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Accept", "application/octet-stream")
	bucketID, _ := strconv.ParseInt(r.URL.Query().Get("bucket"), 10, 64)
	writePointsResponse(w, bucketID)
}

func writePointsResponse(w io.Writer, bucketID int64) {

	var points []point3

	if err := db.Select(&points, `
SELECT addr, var, value, year, month, day, hour, minute, second, millisecond 
FROM series_time 
WHERE bucket_id = ?`, bucketID); err != nil {
		panic(err)
	}

	if lastBucket().BucketID == bucketID {
		var points3 []point3
		muPoints.Lock()
		for _, p := range currentPoints {
			points3 = append(points3, p.point3())
		}
		muPoints.Unlock()
		points = append(points3, points...)
	}

	write := func(n interface{}) {
		if err := binary.Write(w, binary.LittleEndian, n); err != nil {
			panic(err)
		}
	}
	write(uint64(len(points)))
	for _, x := range points {
		write(byte(x.Addr))
		write(uint16(x.Var))
		write(uint16(x.Year))
		write(byte(x.Month))
		write(byte(x.Day))
		write(byte(x.Hour))
		write(byte(x.Minute))
		write(byte(x.Second))
		write(uint16(x.Millisecond))
		write(float64(x.Value))
	}
}


type deletePointsRequest struct {
	BucketID int64
	Addr     modbus.Addr
	Var      modbus.Var
	ValueMinimum,
	ValueMaximum float64
	TimeMinimum,
	TimeMaximum TimeDelphi
}

