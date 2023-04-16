package donutdb

import (
	"bytes"
	"database/sql"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	_ "github.com/mattn/go-sqlite3"
	"github.com/psanford/donutdb/internal/dynamo"
	"github.com/psanford/donutdb/internal/dynamotest"
	"github.com/psanford/donutdb/internal/schemav1"
	"github.com/psanford/sqlite3vfs"
)

func TestDonutDB(t *testing.T) {
	serverInfo, err := dynamotest.SetupDynamoServer()
	if err != nil {
		t.Fatal(err)
	}

	defer serverInfo.Cleanup()

	vfs := New(serverInfo.DB, serverInfo.TableName, WithDefaultSchemaVersion(1))

	err = sqlite3vfs.RegisterVFS("dynamodb", vfs)
	if err != nil {
		t.Fatal(err)
	}

	dbName := fmt.Sprintf("donutdb-test-%d.db", time.Now().UnixNano())
	db, err := sql.Open("sqlite3", dbName+"?vfs=dynamodb")
	if err != nil {
		t.Fatal(err)
	}

	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS foo (
id text NOT NULL PRIMARY KEY,
title text
)`)
	if err != nil {
		t.Fatal(err)
	}

	rows := []FooRow{
		{
			ID:    "415",
			Title: "romantic-swell",
		},
		{
			ID:    "610",
			Title: "ironically-gnarl",
		},
		{
			ID:    "768",
			Title: "biophysicist-straddled",
		},
	}

	for _, row := range rows {
		_, err = db.Exec(`INSERT INTO foo (id, title) values (?, ?)`, row.ID, row.Title)
		if err != nil {
			t.Fatal(err)
		}
	}

	rowIter, err := db.Query(`SELECT id, title from foo order by id`)
	if err != nil {
		t.Fatal(err)
	}

	var gotRows []FooRow

	for rowIter.Next() {
		var row FooRow
		err = rowIter.Scan(&row.ID, &row.Title)
		if err != nil {
			t.Fatal(err)
		}
		gotRows = append(gotRows, row)
	}
	err = rowIter.Close()
	if err != nil {
		t.Fatal(err)
	}

	if !cmp.Equal(rows, gotRows) {
		t.Fatal(cmp.Diff(rows, gotRows))
	}

	err = db.Close()
	if err != nil {
		t.Fatal(err)
	}

	// reopen db
	db, err = sql.Open("sqlite3", dbName+"?vfs=dynamodb")
	if err != nil {
		t.Fatal(err)
	}

	rowIter, err = db.Query(`SELECT id, title from foo order by id`)
	if err != nil {
		t.Fatal(err)
	}

	gotRows = gotRows[:0]

	for rowIter.Next() {
		var row FooRow
		err = rowIter.Scan(&row.ID, &row.Title)
		if err != nil {
			t.Fatal(err)
		}
		gotRows = append(gotRows, row)
	}
	err = rowIter.Close()
	if err != nil {
		t.Fatal(err)
	}

	if !cmp.Equal(rows, gotRows) {
		t.Fatal(cmp.Diff(rows, gotRows))
	}

	err = db.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestAccessDelete(t *testing.T) {
	serverInfo, err := dynamotest.SetupDynamoServer()
	if err != nil {
		t.Fatal(err)
	}

	defer serverInfo.Cleanup()

	vfs := New(serverInfo.DB, serverInfo.TableName, WithDefaultSchemaVersion(1))

	fname := fmt.Sprintf("tearfully-coital-%d", time.Now().UnixNano())

	exists, err := vfs.Access(fname, sqlite3vfs.AccessExists)
	if err != nil {
		t.Fatal(err)
	}

	if exists {
		t.Fatal("File exists prior to being written to")
	}

	writable, err := vfs.Access(fname, sqlite3vfs.AccessReadWrite)
	if err != nil {
		t.Fatal(err)
	}

	if !writable {
		t.Fatal("File path is not writable")
	}

	f, _, err := vfs.Open(fname, 0)
	if err != nil {
		t.Fatal(err)
	}

	data := make([]byte, 3267)
	rand.Read(data)

	_, err = f.WriteAt(data, 3227)
	if err != nil {
		t.Fatal(err)
	}

	err = f.Close()
	if err != nil {
		t.Fatal(err)
	}

	exists, err = vfs.Access(fname, sqlite3vfs.AccessExists)
	if err != nil {
		t.Fatal(err)
	}

	if !exists {
		t.Fatal("File does not exist after writing to it")
	}

	err = vfs.Delete(fname, true)
	if err != nil {
		t.Fatal(err)
	}

	exists, err = vfs.Access(fname, sqlite3vfs.AccessExists)
	if err != nil {
		t.Fatal(err)
	}

	if exists {
		t.Fatal("File still exists after deleting it")
	}
}

func TestReadWriteFile(t *testing.T) {
	serverInfo, err := dynamotest.SetupDynamoServer()
	if err != nil {
		t.Fatal(err)
	}

	defer serverInfo.Cleanup()

	vfs := New(serverInfo.DB, serverInfo.TableName, WithDefaultSchemaVersion(1))

	fname := fmt.Sprintf("undervalues-reverend-%d", time.Now().UnixNano())
	vfsF, _, err := vfs.Open(fname, 0)
	if err != nil {
		t.Fatal(err)
	}
	f := newFsanity(vfsF)
	defer f.Close()

	size, err := f.FileSize()
	if err != nil {
		t.Fatal(err)
	}
	if size != 0 {
		t.Fatalf("Expected new file to have size 0 but was %d", size)
	}

	data := []byte("rustic-grouped")
	n, err := f.WriteAt(data, 0)
	if err != nil {
		t.Fatal(err)
	}
	if n != len(data) {
		t.Fatalf("WriteAt n %d != len(data) %d", n, len(data))
	}

	size, err = f.FileSize()
	if err != nil {
		t.Fatal(err)
	}

	if size != int64(len(data)) {
		t.Fatalf("Filesize != len(data): %d vs %d", size, len(data))
	}

	got := make([]byte, 1024)

	n, err = f.ReadAt(got, 0)
	if err != io.EOF {
		t.Fatal(err)
	}
	got = got[:n]

	if !cmp.Equal(data, got) {
		t.Fatal(cmp.Diff(data, got))
	}

	// read the exact correct size
	n, err = f.ReadAt(got, 0)
	if err != nil {
		t.Fatal(err)
	}

	if !cmp.Equal(data, got) {
		t.Fatal(cmp.Diff(data, got))
	}

	// read less than total data
	got = got[:n-1]
	n, err = f.ReadAt(got, 0)
	if err != nil {
		t.Fatal(err)
	}

	if !cmp.Equal(data[:len(data)-1], got) {
		t.Fatal(cmp.Diff(data, got))
	}

	fname2 := "vomiting-wraith"
	vfsf2, _, err := vfs.Open(fname2, 0)
	if err != nil {
		t.Fatal(err)
	}
	f2 := newFsanity(vfsf2)

	defer f2.Close()

	n, err = f2.WriteAt(data, 32)
	if err != nil {
		t.Fatal(err)
	}
	if n != len(data) {
		t.Fatalf("WriteAt n %d != len(data) %d", n, len(data))
	}

	size, err = f2.FileSize()
	if err != nil {
		t.Fatal(err)
	}

	if size != int64(32+len(data)) {
		t.Fatalf("Filesize != 32+len(data): %d vs %d", size, 32+len(data))
	}

	got = make([]byte, 1024)

	n, err = f2.ReadAt(got, 0)
	if err != nil && err != io.EOF {
		t.Fatal(err)
	}
	got = got[:n]

	expect := make([]byte, 32)
	expect = append(expect, data...)
	if !cmp.Equal(expect, got) {
		t.Fatal(cmp.Diff(expect, got))
	}

	data = make([]byte, 1549516)
	rand.Read(data)
	_, err = f.WriteAt(data, 305204)
	if err != nil {
		t.Fatal(err)
	}

	_, err = f.FileSize()
	if err != nil {
		t.Fatal(err)
	}

	_, err = f.ReadAt(data, 305204)
	if err != nil {
		t.Fatal(err)
	}

	size, err = f.FileSize()
	if err != nil {
		t.Fatal(err)
	}
	err = f.Truncate(305204 + 3679)
	if err != nil {
		t.Fatal(err)
	}
}

func TestReadWriteCases(t *testing.T) {
	serverInfo, err := dynamotest.SetupDynamoServer()
	if err != nil {
		t.Fatal(err)
	}

	defer serverInfo.Cleanup()

	vfs := New(serverInfo.DB, serverInfo.TableName, WithDefaultSchemaVersion(1))

	// these are test cases triggered by simple fuzzing
	// try to convert into a minimal test case when possible
	checks := []writeReadCheck{
		{
			writes: []sizeOff{
				{
					size:   1549516,
					offset: 305204,
				},
			},
		},
		{
			writes: []sizeOff{
				{
					size:   148715,
					offset: 3071696,
				},
			},
		},
		{
			writes: []sizeOff{
				// do a small write,
				// trigger a grow
				// write on a sector size boundry
				{
					offset: 0,
					size:   1,
				},
				{
					offset: dynamo.DefaultSectorSize,
					size:   10,
				},
			},
		},
		{
			writes: []sizeOff{
				// do a small write,
				// trigger a grow
				// write on a sector size boundry
				{
					offset: 0,
					size:   1,
				},
				{
					offset: dynamo.DefaultSectorSize * 2,
					size:   dynamo.DefaultSectorSize,
				},
			},
		},
		{
			writes: []sizeOff{
				{
					offset: 0,
					size:   1,
				},
				{
					offset: 57574,
					size:   10208,
				},
			},
		},
	}

	for i, check := range checks {
		fname := fmt.Sprintf("cashew-discontinuous-%d", i)
		vfsF, _, err := vfs.Open(fname, 0)
		if err != nil {
			t.Fatalf("check: %d, open file err=%s", i, err)
		}
		f := newFsanity(vfsF)

		rand.Seed(check.seed)

		for _, pos := range check.writes {
			data := make([]byte, pos.size)
			rand.Read(data)
			_, err = f.WriteAt(data, pos.offset)
			if err != nil {
				t.Fatalf("check: %d: writeAt err: %s", i, err)
			}

			got := make([]byte, pos.size)
			_, err = f.ReadAt(got, pos.offset)
			if err != nil {
				t.Fatalf("check: %d, readAt err: %s", i, err)
			}

			if !bytes.Equal(data, got) {
				t.Fatalf("check: %d, sanity check written data != read data", i)
			}

			size, err := f.FileSize()
			if err != nil {
				t.Fatal(err)
			}

			// sanity check against local fs copy via readWriteSanity
			fullData := make([]byte, size)
			_, err = f.ReadAt(fullData, 0)
			if err != nil {
				t.Fatal(err)
			}
		}

		f.Close()
	}
}

func TestErrorOnBadSector(t *testing.T) {
	serverInfo, err := dynamotest.SetupDynamoServer()
	if err != nil {
		t.Fatal(err)
	}

	defer serverInfo.Cleanup()

	vfs := New(serverInfo.DB, serverInfo.TableName, WithSectorSize(1024), WithDefaultSchemaVersion(1))

	fname := fmt.Sprintf("theosophic-tempera-%d", time.Now().UnixNano())

	f, _, err := vfs.Open(fname, 0)
	if err != nil {
		t.Fatal(err)
	}
	ff := f.(*schemav1.File)

	data := make([]byte, 458)
	rand.Read(data)

	_, err = f.WriteAt(data, 0)
	if err != nil {
		t.Fatal(err)
	}

	err = ff.SanityCheckSectors()
	if err != nil {
		t.Fatal(err)
	}

	err = f.Close()
	if err != nil {
		t.Fatal(err)
	}

	f, _, err = vfs.Open(fname, 0)
	if err != nil {
		t.Fatal(err)
	}
	ff = f.(*schemav1.File)

	_, err = f.WriteAt(data, 4060)
	if err != nil {
		t.Fatal(err)
	}

	err = ff.SanityCheckSectors()
	if err != nil {
		t.Fatal(err)
	}

	err = f.Close()
	if err != nil {
		t.Fatal(err)
	}

	f, _, err = vfs.Open(fname, 0)
	if err != nil {
		t.Fatal(err)
	}
	ff = f.(*schemav1.File)

	err = ff.SanityCheckSectors()
	if err != nil {
		t.Fatal(err)
	}

	// intentionally write a sector without filling
	// in the intermediate
	secWriter := &schemav1.SectorWriter{
		F: ff,
	}

	rand.Read(data)

	secWriter.WriteSector(&schemav1.Sector{
		Offset: 1 << 20,
		Data:   data,
	})

	err = secWriter.Flush()
	if err != nil {
		t.Fatal(err)
	}

	f.Close()

	f, _, err = vfs.Open(fname, 0)
	if err != nil {
		t.Fatal(err)
	}

	fileSize, err := f.FileSize()
	if err != nil {
		t.Fatal(err)
	}

	reader := io.NewSectionReader(f, 0, fileSize)
	data, err = io.ReadAll(reader)
	if err != dynamo.SectorNotFoundErr {
		t.Errorf("Expected dynamo.SectorNotFoundErr but got %s", err)
	}
	ff = f.(*schemav1.File)

	err = ff.SanityCheckSectors()
	if err == nil {
		t.Fatalf("Expected sanity err but got none")
	}

}

func TestFullPathname(t *testing.T) {
	checks := []struct {
		in     string
		expect string
	}{
		{
			in:     "rancidity-embalmers.db",
			expect: "/rancidity-embalmers.db",
		},
		{
			in:     "/rancidity-embalmers.db",
			expect: "/rancidity-embalmers.db",
		},
		{
			in:     "//rancidity-embalmers.db",
			expect: "/rancidity-embalmers.db",
		},
		{
			in:     "//critical///swapping.db",
			expect: "/critical/swapping.db",
		},
	}

	v := vfs{}

	for _, check := range checks {
		got := v.FullPathname(check.in)
		if got != check.expect {
			t.Fatalf("Check fullpath: in=%s, got=%s expect=%s", check.in, got, check.expect)
		}
	}
}

type FooRow struct {
	ID    string
	Title string
}

func newFsanity(rwa readWriteAt) *readWriteSanity {
	f, err := ioutil.TempFile("", "donutdb-test-sanity")
	if err != nil {
		panic(err)
	}
	return &readWriteSanity{
		primary: rwa,
		sanity:  f,
	}
}

type readWriteSanity struct {
	primary readWriteAt
	sanity  *os.File
}

func (f *readWriteSanity) FileSize() (int64, error) {
	n1, err1 := f.primary.FileSize()

	point, err := f.sanity.Seek(0, io.SeekCurrent)
	if err != nil {
		panic(err)
	}
	n2, err := f.sanity.Seek(0, io.SeekEnd)
	if err != nil {
		panic(err)
	}

	_, err = f.sanity.Seek(point, io.SeekStart)
	if err != nil {
		panic(err)
	}

	if n1 != n2 || err1 != nil {
		panic(fmt.Sprintf("sanity FileSize failed n1=%d n2=%d err1=%s", n1, n2, err1))
	}

	return n1, err1
}

func (f *readWriteSanity) WriteAt(p []byte, off int64) (int, error) {
	n1, err1 := f.primary.WriteAt(p, off)
	n2, err2 := f.sanity.WriteAt(p, off)

	if n1 != n2 || err1 != err2 {
		panic(fmt.Sprintf("sanity WriteAt failed n1=%d n2=%d err1=%s err_sanity=%s", n1, n2, err1, err2))
	}

	return n1, err1
}

func (f *readWriteSanity) ReadAt(p []byte, off int64) (int, error) {
	n1, err1 := f.primary.ReadAt(p, off)
	p2 := make([]byte, len(p))
	n2, err2 := f.sanity.ReadAt(p2, off)

	if n1 != n2 || err1 != err2 {
		panic(fmt.Sprintf("sanity ReadAt failed n1=%d n2=%d err1=%s err_sanity=%s", n1, n2, err1, err2))
	}

	if !bytes.Equal(p[:n1], p2[:n2]) {
		panic("sanity ReadAt p != p2")
	}

	return n1, err1
}

func (f *readWriteSanity) Close() error {
	f.sanity.Close()
	os.Remove(f.sanity.Name())

	err := f.primary.Close()
	return err
}

func (f *readWriteSanity) Truncate(n int64) error {
	err1 := f.primary.Truncate(n)
	err2 := f.sanity.Truncate(n)

	if err1 != err2 {
		panic(fmt.Sprintf("sanity Truncate failed err1=%s err_sanity=%s", err1, err2))
	}

	// trigger sanity filesize check
	f.FileSize()

	return err1
}

type readWriteAt interface {
	io.ReaderAt
	io.WriterAt
	io.Closer
	Truncate(size int64) error
	FileSize() (int64, error)
}

type writeReadCheck struct {
	seed   int64
	writes []sizeOff
}

type sizeOff struct {
	size   int
	offset int64
}
