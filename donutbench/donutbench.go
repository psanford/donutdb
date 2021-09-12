package main

import (
	"database/sql"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/divan/num2words"
	_ "github.com/mattn/go-sqlite3"
	"github.com/psanford/donutdb"
	"github.com/psanford/sqlite3vfs"
)

var (
	mode        = flag.String("mode", "local", "local|donutdb|local-dynamo")
	dynamoTable = flag.String("dynamo-table", "", "Table to use for donutDB")
	region      = flag.String("region", "us-east-1", "AWS Region")
)

func main() {
	flag.Parse()

	if *mode == "local" {
		f, err := ioutil.TempFile("", "donutbench.db")
		if err != nil {
			panic(err)
		}
		name := f.Name()
		f.Close()

		defer os.Remove(name)

		b := benchSuite{
			mode:    *mode,
			connStr: name,
		}
		b.run()
	} else if *mode == "donutdb" {
		name := fmt.Sprintf("/%d.db", time.Now().Nanosecond())

		sess := session.New(&aws.Config{
			Region: region,
		})
		dynamoClient := dynamodb.New(sess)

		clf, err := os.Create("/tmp/donut_change_log")
		if err != nil {
			panic(err)
		}

		vfs := donutdb.New(dynamoClient, *dynamoTable, donutdb.WithSectorSize(4096), donutdb.WithChangeLogWriter(clf))
		err = sqlite3vfs.RegisterVFS("donutdb", vfs)
		if err != nil {
			log.Fatalf("Register VFS err: %s", err)
		}

		defer vfs.Delete(name, true)

		connStr := fmt.Sprintf("file://%s?vfs=donutdb", name)

		b := benchSuite{
			mode:    *mode,
			connStr: connStr,
		}
		b.run()
	} else if *mode == "local-dynamo" {
		serverInfo, err := setupDynamoServer()
		if err != nil {
			log.Fatal(err)
		}
		defer serverInfo.Cleanup()

		dynamoTable = &serverInfo.TableName

		name := fmt.Sprintf("/%d.db", time.Now().Nanosecond())

		clf, err := os.Create("/tmp/donut_change_log")
		if err != nil {
			panic(err)
		}

		vfs := donutdb.New(serverInfo.db, *dynamoTable, donutdb.WithSectorSize(4096), donutdb.WithChangeLogWriter(clf))
		err = sqlite3vfs.RegisterVFS("donutdb", vfs)
		if err != nil {
			log.Fatalf("Register VFS err: %s", err)
		}

		defer vfs.Delete(name, true)

		connStr := fmt.Sprintf("file://%s?vfs=donutdb", name)

		b := benchSuite{
			mode:    *mode,
			connStr: connStr,
		}
		b.run()
	}
}

type benchSuite struct {
	connStr string
	mode    string
	db      *sql.DB
}

func (b *benchSuite) run() {
	checks := []benchmark{
		{
			name:  "open",
			run:   b.open,
			fatal: true,
		},
		{
			name: "insert_1000",
			run:  b.insert,
		},
		{
			name: "insert_25000_tx",
			run:  b.insertTx,
		},
		{
			name: "insert_25000_indexed",
			run:  b.insertIndexed,
		},
		{
			name: "select_without_index",
			run:  b.selectWithoutIndex,
		},
		// {
		// 	name: "select_on_string_comparision",
		// 	run:  b.selectOnStringComparison,
		// },
		// {
		// 	name: "create_index",
		// 	run:  b.createIndex,
		// },
		// {
		// 	name: "select_with_index",
		// 	run:  b.selectWithIndex,
		// },
		// {
		// 	name: "update_without_index",
		// 	run:  b.updateWithoutIndex,
		// },
		// {
		// 	name: "update_with_index",
		// 	run:  b.updateWithIndex,
		// },
		// {
		// 	name: "update_text_with_index",
		// 	run:  b.textUpdateWithIndex,
		// },
		// {
		// 	name: "insert_from_select",
		// 	run:  b.insertFromSelect,
		// },
		// {
		// 	name: "delete_without_index",
		// 	run:  b.deleteWithoutIndex,
		// },
		// {
		// 	name: "delete_with_index",
		// 	run:  b.deleteWithIndex,
		// },
		// {
		// 	name: "big_insert_after_big_delete",
		// 	run:  b.bigInsertAfterBigDelete,
		// },
		// {
		// 	name: "big_delete_after_many_small_inserts",
		// 	run:  b.bigDeleteManySmallInserts,
		// },
		// {
		// 	name: "drop_table",
		// 	run:  b.dropTable,
		// },
	}

	for i, check := range checks {
		d, err := check.run()
		if err != nil && check.fatal {
			log.Fatalf("mode=%s check=%s(%d) fatal_err=%s", b.mode, check.name, i, err)
		} else if err != nil {
			log.Printf("mode=%s check=%s(%d) err=%s", b.mode, check.name, i, err)
			continue
		}

		log.Printf("mode=%s check=%s(%d) took=%dms", b.mode, check.name, i, d.Milliseconds())
	}
}

type benchmark struct {
	name  string
	fatal bool
	run   func() (time.Duration, error)
}

func (b *benchSuite) open() (time.Duration, error) {
	t0 := time.Now()
	db, err := sql.Open("sqlite3", b.connStr)
	delta := time.Since(t0)
	b.db = db
	return delta, err
}

type insertStatement struct {
	b int
	c string
}

// 1000 INSERTs
func (b *benchSuite) insert() (time.Duration, error) {
	inserts := make([]insertStatement, 100)
	for i := 0; i < len(inserts); i++ {
		b := rand.Intn(100000)
		c := num2words.Convert(b)
		inserts[i] = insertStatement{
			b: b,
			c: c,
		}
	}

	t0 := time.Now()
	_, err := b.db.Exec("CREATE TABLE t1(a INTEGER, b INTEGER, c VARCHAR(100))")
	if err != nil {
		return 0, err
	}

	t1 := time.Now()
	for i, s := range inserts {
		_, err = b.db.Exec("INSERT INTO t1 VALUES(?, ?, ?)", i, s.b, s.c)
		if err != nil {
			return 0, err
		}
		if i%10 == 0 {
			log.Printf("== %d took %s", i, time.Since(t1))
			t1 = time.Now()
		}
	}

	return time.Since(t0), nil
}

// 25000 INSERTs in a transaction
func (b *benchSuite) insertTx() (time.Duration, error) {
	inserts := make([]insertStatement, 25000)
	for i := 0; i < len(inserts); i++ {
		b := rand.Intn(500000)
		c := num2words.Convert(b)
		inserts[i] = insertStatement{
			b: b,
			c: c,
		}
	}

	t0 := time.Now()
	_, err := b.db.Exec("CREATE TABLE t2(a INTEGER, b INTEGER, c VARCHAR(100))")
	if err != nil {
		return 0, err
	}

	tx, err := b.db.Begin()
	if err != nil {
		return 0, err
	}

	for i, s := range inserts {
		_, err = tx.Exec("INSERT INTO t2 VALUES(?, ?, ?)", i, s.b, s.c)
		if err != nil {
			return 0, err
		}
	}
	err = tx.Commit()
	return time.Since(t0), err
}

// 25000 INSERTs into an indexed table
func (b *benchSuite) insertIndexed() (time.Duration, error) {
	inserts := make([]insertStatement, 25000)
	for i := 0; i < len(inserts); i++ {
		b := rand.Intn(500000)
		c := num2words.Convert(b)
		inserts[i] = insertStatement{
			b: b,
			c: c,
		}
	}

	t0 := time.Now()
	_, err := b.db.Exec("CREATE TABLE t3(a INTEGER, b INTEGER, c VARCHAR(100))")
	if err != nil {
		return 0, err
	}

	_, err = b.db.Exec("CREATE INDEX i3 ON t3(c)")
	if err != nil {
		return 0, err
	}

	tx, err := b.db.Begin()
	if err != nil {
		return 0, err
	}

	for i, s := range inserts {
		_, err = tx.Exec("INSERT INTO t3 VALUES(?, ?, ?)", i, s.b, s.c)
		if err != nil {
			return 0, err
		}
	}
	err = tx.Commit()
	return time.Since(t0), err
}

// 100 SELECTs without an index
func (b *benchSuite) selectWithoutIndex() (time.Duration, error) {
	t0 := time.Now()

	tx, err := b.db.Begin()
	if err != nil {
		return 0, err
	}

	for i := 0; i < 100; i++ {
		start := i * 100
		end := start + 1000

		r := tx.QueryRow("SELECT count(*), avg(b) from t2 WHERE b >= ? AND b < ?", start, end)
		var count int
		var avg float64
		err = r.Scan(&count, &avg)
		if err != nil {
			return 0, err
		}
	}

	err = tx.Commit()
	return time.Since(t0), err
}

// 100 SELECTs on a string comparison
func (b *benchSuite) selectOnStringComparison() (time.Duration, error) {
	queries := make([]string, 100)
	for i := 0; i < len(queries); i++ {
		q := "SELECT count(*), avg(b) from t2 WHERE c LIKE '%" + num2words.Convert(i+1) + "%'"

		queries[i] = q
	}

	t0 := time.Now()

	tx, err := b.db.Begin()
	if err != nil {
		return 0, err
	}

	for _, q := range queries {
		r := tx.QueryRow(q)
		var count int
		var avg float64
		err = r.Scan(&count, &avg)
		if err != nil {
			return 0, err
		}
	}

	err = tx.Commit()
	return time.Since(t0), err
}

// Creating an index
func (b *benchSuite) createIndex() (time.Duration, error) {
	t0 := time.Now()

	_, err := b.db.Exec("CREATE INDEX i2a ON t2(a)")
	if err != nil {
		return 0, err
	}

	_, err = b.db.Exec("CREATE INDEX i2b ON t2(b)")
	if err != nil {
		return 0, err
	}

	return time.Since(t0), nil
}

// 5000 SELECTs with an index
func (b *benchSuite) selectWithIndex() (time.Duration, error) {
	t0 := time.Now()

	t1 := time.Now()
	for i := 0; i < 500; i++ {
		start := i * 100
		end := start + 100

		r := b.db.QueryRow("SELECT count(*), avg(b) from t2 WHERE b >= ? AND b < ?", start, end)
		var count int
		var avg *float64
		err := r.Scan(&count, &avg)
		if err != nil {
			return 0, err
		}

		if i%10 == 0 {
			log.Printf("== %d took %s", i, time.Since(t1))
			t1 = time.Now()
		}
	}

	return time.Since(t0), nil
}

// 1000 UPDATEs without an index
func (b *benchSuite) updateWithoutIndex() (time.Duration, error) {
	t0 := time.Now()

	tx, err := b.db.Begin()
	if err != nil {
		return 0, err
	}

	for i := 0; i < 1000; i++ {
		start := i * 10
		end := start + 10

		_, err = tx.Exec("UPDATE t1 SET b=b*2 WHERE a>=? AND a<?", start, end)
		if err != nil {
			return 0, err
		}
	}

	err = tx.Commit()
	return time.Since(t0), err
}

// 25000 UPDATEs with an index
func (b *benchSuite) updateWithIndex() (time.Duration, error) {
	t0 := time.Now()

	tx, err := b.db.Begin()
	if err != nil {
		return 0, err
	}

	for i := 0; i < 25000; i++ {
		b := rand.Intn(500000)
		_, err = tx.Exec("UPDATE t2 SET b=? WHERE a=?", b, i+1)
		if err != nil {
			return 0, err
		}
	}

	err = tx.Commit()
	return time.Since(t0), err
}

// 25000 text UPDATEs with an index
func (b *benchSuite) textUpdateWithIndex() (time.Duration, error) {
	inserts := make([]insertStatement, 25000)
	for i := 0; i < len(inserts); i++ {
		inserts[i] = insertStatement{
			c: num2words.Convert(rand.Intn(500000)),
		}
	}

	t0 := time.Now()

	tx, err := b.db.Begin()
	if err != nil {
		return 0, err
	}

	for i, s := range inserts {
		_, err = tx.Exec("UPDATE t2 SET c=? WHERE a=?", i, s.c)
		if err != nil {
			return 0, err
		}
	}
	err = tx.Commit()
	return time.Since(t0), err
}

// INSERTs from a SELECT
func (b *benchSuite) insertFromSelect() (time.Duration, error) {
	t0 := time.Now()

	tx, err := b.db.Begin()
	if err != nil {
		return 0, err
	}

	_, err = tx.Exec("INSERT INTO t1 SELECT b,a,c FROM t2")
	if err != nil {
		return 0, err
	}

	_, err = tx.Exec("INSERT INTO t2 SELECT b,a,c FROM t1")
	if err != nil {
		return 0, err
	}

	err = tx.Commit()
	return time.Since(t0), err
}

// DELETE without an index
func (b *benchSuite) deleteWithoutIndex() (time.Duration, error) {
	fifty := num2words.Convert(50)

	t0 := time.Now()

	_, err := b.db.Exec("DELETE FROM t2 WHERE c LIKE '%" + fifty + "%'")
	return time.Since(t0), err
}

// DELETE with an index
func (b *benchSuite) deleteWithIndex() (time.Duration, error) {
	t0 := time.Now()

	_, err := b.db.Exec("DELETE FROM t2 WHERE a>10 AND a<20000")
	return time.Since(t0), err
}

// A big INSERT after a big DELETE
func (b *benchSuite) bigInsertAfterBigDelete() (time.Duration, error) {
	t0 := time.Now()

	_, err := b.db.Exec("INSERT INTO t2 SELECT * FROM t1")
	return time.Since(t0), err
}

// A big DELETE followed by many small INSERTs
func (b *benchSuite) bigDeleteManySmallInserts() (time.Duration, error) {
	inserts := make([]insertStatement, 12000)
	for i := 0; i < len(inserts); i++ {
		b := rand.Intn(100000)
		c := num2words.Convert(b)
		inserts[i] = insertStatement{
			b: b,
			c: c,
		}
	}

	t0 := time.Now()

	tx, err := b.db.Begin()
	if err != nil {
		return 0, err
	}

	_, err = tx.Exec("DELETE FROM t1")
	if err != nil {
		return 0, err
	}

	for i, s := range inserts {
		_, err = tx.Exec("INSERT INTO t1 VALUES(?, ?, ?)", i, s.b, s.c)
		if err != nil {
			return 0, err
		}
	}
	err = tx.Commit()
	return time.Since(t0), err
}

// DROP TABLE
func (b *benchSuite) dropTable() (time.Duration, error) {
	t0 := time.Now()

	_, err := b.db.Exec("DROP TABLE t1")
	if err != nil {
		return 0, err
	}

	_, err = b.db.Exec("DROP TABLE t2")
	if err != nil {
		return 0, err
	}

	_, err = b.db.Exec("DROP TABLE t3")
	if err != nil {
		return 0, err
	}

	return time.Since(t0), nil
}
