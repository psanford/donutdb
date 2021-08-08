# donutdb: A sqlite VFS backed by DynamoDB

Store and query a sqlite database directly from DynamoDB.

Project Status: Pre-Alpha

## DynamoDB Schema

The basic idea is that all data and metadata will be stored in a
single dynamodb table for the vfs. The goal of using a single dynamodb
table is to make the setup as easy as possible.

The current plan for the schema is as follows:

Dynamo Table:
  PrimaryKey string
  SortKey    int


Data Types stored in the dynamo table:

- File System metadata
This is a list of all files on the filesystem. This will be stored
in a single primary key named "files" with a sort key of 0. Each
attribute key will be a file path and the value will be any additional
metadata stored for that file.

- File data
This is where the bytes for each file is stored. The primary key for a
file will be "fs-${filename}". Each file will be split into 4k
chunks (sectors). The Sort Key is the position in the file at the
sector boundary. If a sector exists, all previous sectors must also
exist in the table. The bytes for a sector are stored in the attribute
named "bytes". That attribute must have exactly 4k bytes, unless it is
the final sector. The final sector should stop where the file stops.

- Lock data
This is where looks are stored for coordination. The primary key for a
file lock will be "lock-${filename}". The sort key will indicate the
type of lock.
