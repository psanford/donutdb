package donutdb

import (
	"database/sql"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/psanford/sqlite3vfs"
)

func TestConcurrentAccess(t *testing.T) {
	serverInfo, err := setupDynamoServer()
	if err != nil {
		t.Fatal(err)
	}

	origRenewDuration := renewDuration
	renewDuration = 50 * time.Millisecond
	defer func() {
		renewDuration = origRenewDuration
	}()

	defer serverInfo.Cleanup()

	vfs1 := New(serverInfo.db, serverInfo.TableName)

	err = sqlite3vfs.RegisterVFS("dynamodb1", vfs1)
	if err != nil {
		t.Fatal(err)
	}

	vfs2 := New(serverInfo.db, serverInfo.TableName)
	err = sqlite3vfs.RegisterVFS("dynamodb2", vfs2)
	if err != nil {
		t.Fatal(err)
	}

	dbName := fmt.Sprintf("donutdb-test-%d.db", time.Now().UnixNano())
	db0, err := sql.Open("sqlite3", dbName+"?vfs=dynamodb1")
	if err != nil {
		t.Fatal(err)
	}

	defer db0.Close()

	_, err = db0.Exec(`CREATE TABLE IF NOT EXISTS concurrent_tbl (
id text NOT NULL PRIMARY KEY,
count int
)`)
	if err != nil {
		t.Fatal(err)
	}

	id := fmt.Sprintf("id-%d", time.Now().Nanosecond())
	_, err = db0.Exec("INSERT INTO concurrent_tbl (id, count) values (?, ?)", id, 0)
	if err != nil {
		t.Fatal(err)
	}

	db1, err := sql.Open("sqlite3", dbName+"?vfs=dynamodb2")
	if err != nil {
		t.Fatal(err)
	}

	defer db1.Close()

	doAtomicUpdates := func(db *sql.DB, connIdx, count int) error {
		for i := 0; i < count; i++ {
			tx, err := db.Begin()
			if err != nil {
				return err
			}
			var oldCount int
			r := tx.QueryRow("SELECT count from concurrent_tbl where id = ?", id)

			err = r.Scan(&oldCount)
			if err != nil {
				tx.Rollback()
				return err
			}

			time.Sleep(time.Duration(rand.Int31n(200) * int32(time.Millisecond)))

			_, err = tx.Exec("UPDATE concurrent_tbl set count = ? where id = ?", oldCount+1, id)
			if err != nil {
				tx.Rollback()
				return err
			}

			err = tx.Commit()
			if err != nil {
				return err
			}
			time.Sleep(time.Duration(rand.Int31n(200) * int32(time.Millisecond)))
		}
		return nil
	}

	resultChan := make(chan error)

	go func() {
		err := doAtomicUpdates(db0, 0, 10)
		resultChan <- err
	}()

	err = doAtomicUpdates(db1, 1, 10)
	if err != nil {
		t.Fatal(err)
	}

	err = <-resultChan
	if err != nil {
		t.Fatal(err)
	}

	r := db0.QueryRow("SELECT count from concurrent_tbl where id = ?", id)

	var finalCount int
	err = r.Scan(&finalCount)
	if err != nil {
		t.Fatal(err)
	}

	if finalCount != 20 {
		t.Fatalf("finalCount got=%d expected=%d", finalCount, 20)
	}

}
