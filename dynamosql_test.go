package donutdb

import (
	"bytes"
	"database/sql"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/google/go-cmp/cmp"
	_ "github.com/mattn/go-sqlite3"
	"github.com/psanford/sqlite3vfs"
)

func setupDynamoServer() (*dynamoServerInfo, error) {
	info := dynamoServerInfo{
		TableName: os.Getenv("DONUTDB_DYNAMODB_TEST_TABLE_NAME"),
		Addr:      os.Getenv("DONUTDB_DYNAMODB_TEST_ADDR"),
		Region:    os.Getenv("DONUTDB_DYNAMODB_TEST_REGION"),
	}

	// if set, test will try to start local dynamo db jar
	dynamoLocalDir := os.Getenv("DONUTDB_DYNAMODB_LOCAL_DIR")

	// XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
	dynamoLocalDir = filepath.Join(os.Getenv("HOME"), "lib/dynamodb_local")

	var cleanups []func()
	info.Cleanup = func() {
		for _, cleanupFunc := range cleanups {
			cleanupFunc()
		}
	}

	if dynamoLocalDir != "" {
		log.Printf("Starting local dyamodb server")
		cmd := exec.Command("java", "-Djava.library.path="+filepath.Join(dynamoLocalDir, "DynamoDBLocal_lib"), "-jar", filepath.Join(dynamoLocalDir, "DynamoDBLocal.jar"), "-sharedDb")
		err := cmd.Start()
		if err != nil {
			return nil, err
		}
		cleanups = append(cleanups, func() {
			cmd.Process.Kill()
		})

		if info.Addr == "" {
			info.Addr = "http://localhost:8000"
		}
		if info.Region == "" {
			info.Region = "us-east-2"
		}

		os.Setenv("AWS_ACCESS_KEY_ID", "fakeMyKeyId")
		os.Setenv("AWS_SECRET_ACCESS_KEY", "fakeSecretAccessKey")

		deadline := time.Now().Add(5 * time.Second)
		var connectOK bool
		for time.Now().Before(deadline) {
			resp, err := http.Get(info.Addr)
			if err != nil {
				time.Sleep(1 * time.Millisecond)
				continue
			}
			resp.Body.Close()
			connectOK = true
			break
		}

		if !connectOK {
			return nil, fmt.Errorf("Failed to conncet to test dynamodb server within deadline")
		}
	}

	if info.Region == "" {
		return nil, fmt.Errorf("Missing required environment variables to connect to dynamodb (either local or remote)")
	}

	sess := session.Must(session.NewSessionWithOptions(session.Options{
		Config: aws.Config{
			Region:     &info.Region,
			Endpoint:   &info.Addr,
			MaxRetries: aws.Int(0),
			// LogLevel: aws.LogLevel(aws.LogDebug),
			// Logger:   aws.NewDefaultLogger(),
		},
	}))
	info.db = dynamodb.New(sess)

	if info.TableName == "" {
		info.TableName = fmt.Sprintf("donutdb-test-%d", time.Now().UnixNano())

		_, err := info.db.CreateTable(&dynamodb.CreateTableInput{
			TableName: &info.TableName,
			AttributeDefinitions: []*dynamodb.AttributeDefinition{
				{
					AttributeName: aws.String("hash_key"),
					AttributeType: aws.String("S"),
				},
				{
					AttributeName: aws.String("range_key"),
					AttributeType: aws.String("N"),
				},
			},
			KeySchema: []*dynamodb.KeySchemaElement{
				{
					AttributeName: aws.String("hash_key"),
					KeyType:       aws.String("HASH"),
				},
				{
					AttributeName: aws.String("range_key"),
					KeyType:       aws.String("RANGE"),
				},
			},
			BillingMode: aws.String("PAY_PER_REQUEST"),
		})

		if err != nil {
			return nil, err
		}
	}

	return &info, nil
}

type dynamoServerInfo struct {
	Region    string
	Addr      string
	TableName string
	Cleanup   func()
	db        *dynamodb.DynamoDB
}

func TestDonutDB(t *testing.T) {
	serverInfo, err := setupDynamoServer()
	if err != nil {
		t.Fatal(err)
	}

	defer serverInfo.Cleanup()

	vfs := New(serverInfo.db, serverInfo.TableName)

	err = sqlite3vfs.RegisterVFS("dynamodb", vfs)
	if err != nil {
		t.Fatal(err)
	}

	dbName := fmt.Sprintf("donutdb-test-%d.db", time.Now().UnixNano())
	db, err := sql.Open("sqlite3", dbName+"?vfs=dynamodb")
	if err != nil {
		t.Fatal(err)
	}

	defer db.Close()

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
}

func TestReadWriteFile(t *testing.T) {
	serverInfo, err := setupDynamoServer()
	if err != nil {
		t.Fatal(err)
	}

	defer serverInfo.Cleanup()

	vfs := New(serverInfo.db, serverInfo.TableName)

	fname := "undervalues-reverend"
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
}

func TestReadWriteCases(t *testing.T) {
	serverInfo, err := setupDynamoServer()
	if err != nil {
		t.Fatal(err)
	}

	defer serverInfo.Cleanup()

	vfs := New(serverInfo.db, serverInfo.TableName)

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
					offset: 4096,
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
					offset: 8192,
					size:   4096,
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

	if !reflect.DeepEqual(p[:n1], p2[:n2]) {
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

type readWriteAt interface {
	io.ReaderAt
	io.WriterAt
	io.Closer
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
