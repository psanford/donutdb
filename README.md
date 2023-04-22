# DonutDB: A SQL database implemented on DynamoDB and SQLite

Store and query a sqlite database directly backed by DynamoDB.

Project Status: Alpha

## What is this?

This is a SQL database backed only by DynamoDB. More specifically,
this is a SQLite VFS (virtual file system) implemented on top of DynamoDB.

## But why?

I believe this is the cheapest way to run a SQL database in AWS (at least
for small databases with relatively low query volumes).

My main use case is to have a read/write SQL database that can be used
by AWS Lambda functions. It could also be used by software running on
EC2 instances.

## How do I use this?

You need to create a DynamoDB table with the following properties:

```
resource "aws_dynamodb_table" "table" {
  name         = "some-dynamo-table-name"
  hash_key     = "hash_key"
  range_key    = "range_key"

  attribute {
    name = "hash_key"
    type = "S"
  }

  attribute {
    name = "range_key"
    type = "N"
  }
}

```

Then to use in a Go application:

```
package main

import (
	"database/sql"
	"fmt"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	_ "github.com/mattn/go-sqlite3"
	"github.com/psanford/sqlite3vfs"
)

func main() {

	dynamoDBclient := dynamodb.New(session.New())

	tableName := "some-dynamo-table-name"

	vfs, err := donutdb.New(dynamoDBclient, tableName)
	if err != nil {
		panic(err)
	}

	// register the custom donutdb vfs with sqlite
	// the name specifed here must match the `vfs` param
	// passed to sql.Open in the dataSourceName:
	// e.g. `...?vfs=donutdb`
	err = sqlite3vfs.RegisterVFS("donutdb", vfs)
	if err != nil {
		panic(err)
	}

	// file0 is the name of the file stored in dynamodb
	// you can have multiple db files stored in a single dynamodb table
	// The `vfs=donutdb` instructs sqlite to use the custom vfs implementation.
	// The name must match the name passed to `sqlite3vfs.RegisterVFS`
	db, err := sql.Open("sqlite3", "file0.db?vfs=donutdb")
	if err != nil {
		panic(err)
	}
	defer db.Close()

	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS foo (
id text NOT NULL PRIMARY KEY,
title text
)`)
	if err != nil {
		panic(err)
	}

	_, err = db.Exec(`INSERT INTO foo (id, title) values (?, ?)`, "developer-arbitration", "washroom-whitecap")
	if err != nil {
		panic(err)
	}

	var gotID, gotTitle string
	row := db.QueryRow("SELECT id, title FROM foo where id = ?", "developer-arbitration")
	err = row.Scan(&gotID, &gotTitle)
	if err != nil {
		panic(err)
	}

	fmt.Printf("got: id=%s title=%s", gotID, gotTitle)
}
```

### SQLite3 CLI loadable module

DonutDB also has a SQLite3 module in `donutdb-loadable`. This allows you to interact with DonutDB databases interactively from the SQLite3 CLI.

```
$ cd donutdb-loadable
$ make
go build -tags SQLITE3VFS_LOADABLE_EXT -o donutloadable.a -buildmode=c-archive donut_loadable.go
rm donutloadable.h
gcc -g -fPIC -shared -o donutdb.so donutloadable.c donutloadable.a

# set the DynamoDB table name:
$ export DONUTDB_TABLE=my-donutdb-table

$ sqlite3
SQLite version 3.31.1 2020-01-27 19:55:54
Enter ".help" for usage hints.
sqlite> -- load extension
sqlite> .load ./donutdb
sqlite3vfs register donutdb
sqlite3vfs register donutdb done
sqlite> -- open db using vfs=donutdb, note you must use the sqlite uri syntax which starts with file://
sqlite> .open file:///foo.db?vfs=donutdb
sqlite> -- query from remote db
sqlite> SELECT * from csv where period > '2010' limit 10;
series_reference      period      data_value  suppressed  status      units       magntude    subject                    grp                                                   series_title_1
--------------------  ----------  ----------  ----------  ----------  ----------  ----------  -------------------------  ----------------------------------------------------  --------------
BOPQ.S06AC000000000A  2010.03     17463                   F           Dollars     6           Balance of Payments - BOP  BPM6 Quarterly, Balance of payments major components  Actual
BOPQ.S06AC000000000A  2010.06     17260                   F           Dollars     6           Balance of Payments - BOP  BPM6 Quarterly, Balance of payments major components  Actual
BOPQ.S06AC000000000A  2010.09     15419                   F           Dollars     6           Balance of Payments - BOP  BPM6 Quarterly, Balance of payments major components  Actual
BOPQ.S06AC000000000A  2010.12     17088                   F           Dollars     6           Balance of Payments - BOP  BPM6 Quarterly, Balance of payments major components  Actual
BOPQ.S06AC000000000A  2011.03     18516                   F           Dollars     6           Balance of Payments - BOP  BPM6 Quarterly, Balance of payments major components  Actual
BOPQ.S06AC000000000A  2011.06     18835                   F           Dollars     6           Balance of Payments - BOP  BPM6 Quarterly, Balance of payments major components  Actual
BOPQ.S06AC000000000A  2011.09     16390                   F           Dollars     6           Balance of Payments - BOP  BPM6 Quarterly, Balance of payments major components  Actual
BOPQ.S06AC000000000A  2011.12     18748                   F           Dollars     6           Balance of Payments - BOP  BPM6 Quarterly, Balance of payments major components  Actual
BOPQ.S06AC000000000A  2012.03     18477                   F           Dollars     6           Balance of Payments - BOP  BPM6 Quarterly, Balance of payments major components  Actual
BOPQ.S06AC000000000A  2012.06     18270                   F           Dollars     6           Balance of Payments - BOP  BPM6 Quarterly, Balance of payments major components  Actual
```

### CLI tool

DonutDB also provides a CLI tool to make it easier to manage SQLite database files in DynamoDB. `donutdb-cli` allow you to push and pull db files to a DynamoDB table:

```
$ ./donutdb-cli
DonutDB CLI

Usage:
  donutdb-cli [command]

Available Commands:
  completion  generate the autocompletion script for the specified shell
  debug       Debug commands
  help        Help about any command
  ls          List files in table
  pull        Pull file from DynamoDB to local filesystem
  push        Push file from local filesystem to DynamoDB
  rm          Remove file from dynamodb table

Flags:
  -h, --help   help for donutdb-cli

Use "donutdb-cli [command] --help" for more information about a command.
```

## Is it safe to use concurrently?

It should be. DonutDB currently implements a global lock using
DynamoDB locks. This means access to the database is serialized to a
single client at a time, which should make it safe for multiple
clients without risk of corrupting the data. However this also means
client may get "Database locked" errors if clients hold locks for too
long.

In the future we may implement a multi-reader single-writer locking strategy.

## Performance Considerations

Roundtrip latency to DynamoDB has a major impact on query performance. You probably want to run you application in the same region as your DynamoDB table.

If you are using DonutDB from a Lambda function, you may want to do some testing with how the Lambda function's allocated memory affects query latency (memory size for Lambda also affects cpu allocation). In my testing I've found that at very low memory (128mb) application latency is affected by GC and CPU overhead for zstd decompression. Performance gets significantly better as memory size is increased.

## DynamoDB Schema

The basic idea is that all data and metadata will be stored in a
single dynamodb table for the vfs. The goal of using a single dynamodb
table is to make the setup as easy as possible.

The current plan for the schema is as follows:

Dynamo Table:
  HashKey string
  SortKey    int


Data Types stored in the dynamo table:

V1 schame:
- File metadata
This contains a mapping of filename to metadata. Each file gets a random\_id
that is part of the hash\_key for the file data and lock row. The random\_id
allows for deleting a file atomically by simply removing the metadata record.
The metadata also includes the sector size used for the file. This allows for
changing the sector size default in the future without breaking existing file
records. File metadata is stored in a single row with a hash\_key of
`file-meta-v1` and a range\_key of `0`. The filename is the attribute name
and the metadata is stored as JSON in the attribute value.

- File data
This is where the bytes for each file is stored. The primary key for a
file will be `file-v1-${rand_id}-${filename}`. Each file will be split into 4k
chunks (sectors). The Sort Key is the position in the file at the
sector boundary. If a sector exists, all previous sectors must also
exist in the table. The bytes for a sector are stored in the attribute
named "bytes". That attribute must have exactly 4k bytes, unless it is
the final sector. The final sector should stop where the file stops.

- Lock data
This is where looks are stored for coordination. The current implementation
uses a single global lock, similar to the sqlite `flock` and `dot-lock`
implementations. The primary key for the global lock is
`lock-global-v1-${rand_id}-${filename}` with a sort key of `0`.

It should be possible to implement multi-reader single writer locks on
top of dynamodb in the future.


v2 schema:

In V2 we move sectors into their own individual partitions. This allows
us to cache unchanged sectors easily.

- File metadata
This contains a mapping of filename to metadata. Each file gets a random\_id
that is part of the hash\_key for the file data and lock row. The random\_id
allows for deleting a file atomically by simply removing the metadata record.
The metadata also includes the sector size used for the file. This allows for
changing the sector size default in the future without breaking existing file
records. File metadata is stored in a single row with a hash\_key of
`file-meta-v1` and a range\_key of `0`. The filename is the attribute name
and the metadata is stored as JSON in the attribute value.
In V2 the metadata also includes the full list of sectors and their ids.


- File data
This is where the bytes for each file is stored. The primary key for a
file will be `file-v2-${rand_id}-${filename}-${sector_id}`.
The sector\_id is `${sector\_idx}\_\_${sector\_hash}`. We include
the idx to make it easier for deciding when its safe to delete
unused sectors. The hash allows us to optionally cache sectors.

If a sector exists, all previous sectors must also exist in the table.
The bytes for a sector are stored in the attribute named "bytes". That
attribute must have exactly 4k bytes, unless it is the final sector.
The final sector should stop where the file stops.

- Lock data
This is where looks are stored for coordination. The current implementation
uses a single global lock, similar to the sqlite `flock` and `dot-lock`
implementations. The primary key for the global lock is
`lock-global-v1-${rand_id}-${filename}` with a sort key of `0`.

It should be possible to implement multi-reader single writer locks on
top of dynamodb in the future.
