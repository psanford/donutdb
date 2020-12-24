package donutsql

import (
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/Masterminds/squirrel"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/lann/builder"
	"github.com/psanford/donutdb/internal/donuterr"
	"github.com/twmb/murmur3"
)

const (
	defaultHashFunction = "murmur3_64"
	metadataTableName   = "__donutdb_table_metadata"
)

type DB struct {
	db *sql.DB
}

func New(db *sql.DB) *DB {
	return &DB{db}
}

func (db *DB) Init() error {
	_, err := db.db.Exec(`CREATE TABLE IF NOT EXISTS __donutdb_global_state (
name TEXT NOT NULL PRIMARY KEY,
value
)`)
	if err != nil {
		return err
	}

	row := db.db.QueryRow("SELECT value from __donutdb_global_state where name = ?", "schema_version")
	var version int
	err = row.Scan(&version)
	if err != nil {
		_, err = db.db.Exec("INSERT INTO __donutdb_global_state (name, value) values (?, ?)", "schema_version", 1)
		if err != nil {
			return err
		}
	}

	_, err = db.db.Exec(`CREATE TABLE IF NOT EXISTS __donutdb_table_metadata (
name TEXT NOT NULL PRIMARY KEY,
creation_epoch INTEGER,
hash_key TEXT,
hash_key_type TEXT,
range_key TEXT,
range_key_type TEXT,
hash_function TEXT
)`)

	return err
}

type TableMetadata struct {
	Name          string
	CreationEpoch int64
	HashKey       string
	HashKeyType   string
	RangeKey      string
	RangeKeyType  string
	HashFunction  string
}

func (db *DB) TableMetadata(table string) (*TableMetadata, error) {
	row := db.db.QueryRow("SELECT name,creation_epoch,hash_key,hash_key_type,range_key,range_key_type,hash_function FROM __donutdb_table_metadata where name = ?", table)

	var tbl TableMetadata
	err := row.Scan(&tbl.Name, &tbl.CreationEpoch, &tbl.HashKey, &tbl.HashKeyType, &tbl.RangeKey, &tbl.RangeKeyType, &tbl.HashFunction)
	if err != nil {
		return nil, err
	}

	return &tbl, nil
}

type CreateTableArgs struct {
}

func dynamoTypeToSQLType(dynamoType string) (string, error) {
	switch dynamoType {
	case "S":
		return "TEXT", nil
	case "N":
		return "REAL", nil
	case "B":
		return "BLOB", nil
	}

	return "", fmt.Errorf("Unsupported key type %q", dynamoType)

}

func (db *DB) CreateTable(tableName, hashKey, hashKeyDynamoType, rangeKey, rangeKeyDynamoType string) (*TableMetadata, error) {
	hashKeyType, err := dynamoTypeToSQLType(hashKeyDynamoType)
	if err != nil {
		return nil, err
	}
	var rangeKeyType string

	if rangeKey == "" {
		stmtTxt := fmt.Sprintf(`CREATE TABLE '%s' (
    donutdb_hash_key TEXT PRIMARY KEY,
    '%s' '%s',
    donutdb_data BLOB
  )`, tableName, hashKey, hashKeyType)
		// use prepare to avoid executing more than one statement (a.la sql injection)
		stmt, err := db.db.Prepare(stmtTxt)
		if err != nil {
			return nil, fmt.Errorf("create table err: %q %w", stmtTxt, err)
		}
		_, err = stmt.Exec()
		if err != nil {
			return nil, fmt.Errorf("create table err: %q %w", stmtTxt, err)
		}

	} else {
		rangeKeyType, err = dynamoTypeToSQLType(rangeKeyDynamoType)
		if err != nil {
			return nil, err
		}

		stmtTxt := fmt.Sprintf(`CREATE TABLE '%s' (
    donutdb_hash_key TEXT,
    '%s' %s,
    '%s' %s NOT NULL,
    donutdb_data BLOB,
    PRIMARY KEY (donutdb_hash_key, '%s')
  )`, tableName, hashKey, hashKeyType, rangeKey, rangeKeyType, hashKey)
		// use prepare to avoid executing more than one statement (a.la sql injection)
		stmt, err := db.db.Prepare(stmtTxt)
		if err != nil {
			return nil, fmt.Errorf("create table err: %q %w", stmtTxt, err)
		}
		_, err = stmt.Exec()
		if err != nil {
			return nil, fmt.Errorf("create table err: %w", err)
		}

		stmt, err = db.db.Prepare(fmt.Sprintf("CREATE INDEX range_key_idx on '%s' ('%s')", tableName, rangeKey))
		if err != nil {
			return nil, fmt.Errorf("create table err: %w", err)
		}
		_, err = stmt.Exec()
		if err != nil {
			return nil, fmt.Errorf("create table err: %w", err)
		}
	}

	creationEpoch := time.Now().Unix()

	_, err = squirrel.Insert(metadataTableName).Columns(
		"name", "creation_epoch", "hash_key", "hash_key_type", "range_key", "range_key_type", "hash_function",
	).Values(
		tableName, creationEpoch, hashKey, hashKeyType, rangeKey, rangeKeyType, defaultHashFunction,
	).RunWith(db.db).Exec()
	if err != nil {
		return nil, fmt.Errorf("update metadata err: %w", err)
	}

	out := TableMetadata{
		Name:          tableName,
		CreationEpoch: creationEpoch,
		HashKey:       hashKey,
		HashKeyType:   hashKeyType,
		RangeKey:      rangeKey,
		RangeKeyType:  rangeKeyType,
		HashFunction:  defaultHashFunction,
	}

	return &out, nil
}

func (db *DB) ListTables() ([]string, error) {
	rows, err := squirrel.Select("name").From(metadataTableName).OrderBy("name").RunWith(db.db).Query()
	if err != nil {
		return nil, err
	}

	var tables []string

	for rows.Next() {
		var name string
		err = rows.Scan(&name)
		if err != nil {
			return nil, err
		}
		tables = append(tables, name)
	}
	err = rows.Close()
	if err != nil {
		return nil, err
	}

	return tables, nil
}

type Item map[string]*dynamodb.AttributeValue

// Insert an item into the database.
// If the table has a range key, the range key attribute is required, otherwise it should be nil.
// Returns the previous item if one existed.
// Returns donuterr.ResourceNotFoundErr if the item does not exist.
func (db *DB) Insert(tbl *TableMetadata, hashKeyAttr, rangeKeyAttr *dynamodb.AttributeValue, item Item) (Item, error) {
	if tbl.RangeKey != "" && rangeKeyAttr == nil {
		return nil, fmt.Errorf("range key is required")
	}
	hashedKey, keyVal, err := hashKeyBytes(hashKeyAttr, tbl.HashKeyType)
	if err != nil {
		return nil, fmt.Errorf("hash key err: %w", err)
	}

	tx, err := db.db.Begin()
	if err != nil {
		return nil, err
	}

	shouldRollback := true
	defer func() {
		if shouldRollback {
			tx.Rollback()
		}
	}()

	var oldItem map[string]*dynamodb.AttributeValue

	where := squirrel.Eq{
		"donutdb_hash_key": hashedKey,
		tbl.HashKey:        keyVal,
	}

	var rangeKey interface{}
	if tbl.RangeKey != "" {
		rangeKey, err = rangeKeyI(rangeKeyAttr, tbl.RangeKeyType)
		if err != nil {
			return nil, fmt.Errorf("range key err: %w", err)
		}

		where[tbl.RangeKey] = rangeKey
	}

	query, queryArgs, err := squirrel.Select("donutdb_data").From(tbl.Name).Where(where).ToSql()
	if err != nil {
		return nil, err
	}

	row := tx.QueryRow(query, queryArgs...)
	var oldItemJSON []byte
	err = row.Scan(&oldItemJSON)
	if err == sql.ErrNoRows {
	} else if err != nil {
		return nil, err
	} else {
		err := json.Unmarshal(oldItemJSON, &oldItem)
		if err != nil {
			return nil, fmt.Errorf("corrupt old item in db: %w", err)
		}
	}

	insertArgs := []interface{}{
		hashedKey,
		keyVal,
	}

	if tbl.RangeKey != "" {
		insertArgs = append(insertArgs, rangeKey)
	}

	marshalledItem, err := json.Marshal(item)
	if err != nil {
		return nil, fmt.Errorf("marshal item err: %s", err)
	}
	insertArgs = append(insertArgs, marshalledItem)

	stmt, _, err := insertOrReplace(tbl.Name).Values(insertArgs...).ToSql()
	if err != nil {
		return nil, err
	}

	tx.Exec(stmt, insertArgs...)
	shouldRollback = false
	err = tx.Commit()
	if err != nil {
		return nil, fmt.Errorf("commit data err: %w", err)
	}

	return Item(oldItem), nil
}

// Get a single item from the database.
// If the table has a range key, the range key attribute is required, otherwise it should be nil.
// Returns donuterr.ResourceNotFoundErr if the item does not exist.
func (db *DB) Get(tbl *TableMetadata, hashKeyAttr, rangeKeyAttr *dynamodb.AttributeValue) (Item, error) {
	if tbl.RangeKey != "" && rangeKeyAttr == nil {
		return nil, fmt.Errorf("range key is required")
	}

	hashedKey, keyVal, err := hashKeyBytes(hashKeyAttr, tbl.HashKeyType)
	if err != nil {
		return nil, fmt.Errorf("hash key err: %w", err)
	}

	where := squirrel.Eq{
		"donutdb_hash_key": hashedKey,
		tbl.HashKey:        keyVal,
	}

	if tbl.RangeKey != "" {
		rangeKey, err := rangeKeyI(rangeKeyAttr, tbl.RangeKeyType)
		if err != nil {
			return nil, fmt.Errorf("range key err: %w", err)
		}

		where[tbl.RangeKey] = rangeKey
	}

	query, args, err := squirrel.Select("donutdb_data").From(tbl.Name).Where(where).ToSql()
	if err != nil {
		return nil, err
	}

	var item map[string]*dynamodb.AttributeValue
	row := db.db.QueryRow(query, args...)
	var itemJSON []byte
	err = row.Scan(&itemJSON)
	if err == sql.ErrNoRows {
		return nil, donuterr.ResourceNotFoundErr("item not found")
	} else if err != nil {
		return nil, err
	}

	err = json.Unmarshal(itemJSON, &item)
	if err != nil {
		return nil, fmt.Errorf("corrupt item in db: %w", err)
	}

	return item, nil
}

// Delete an item from the database.
// If the table has a range key, the range key attribute is required, otherwise it should be nil.
// Returns donuterr.ResourceNotFoundErr if the item does not exist.
func (db *DB) Delete(tbl *TableMetadata, hashKeyAttr, rangeKeyAttr *dynamodb.AttributeValue) (Item, error) {
	if tbl.RangeKey != "" && rangeKeyAttr == nil {
		return nil, fmt.Errorf("range key is required")
	}
	hashedKey, keyVal, err := hashKeyBytes(hashKeyAttr, tbl.HashKeyType)
	if err != nil {
		return nil, fmt.Errorf("hash key err: %w", err)
	}

	tx, err := db.db.Begin()
	if err != nil {
		return nil, err
	}
	shouldRollback := true
	defer func() {
		if shouldRollback {
			tx.Rollback()
		}
	}()

	var oldItem map[string]*dynamodb.AttributeValue

	where := squirrel.Eq{
		"donutdb_hash_key": hashedKey,
		tbl.HashKey:        keyVal,
	}

	var rangeKey interface{}
	if tbl.RangeKey != "" {
		rangeKey, err = rangeKeyI(rangeKeyAttr, tbl.RangeKeyType)
		if err != nil {
			return nil, fmt.Errorf("range key err: %w", err)
		}

		where[tbl.RangeKey] = rangeKey
	}

	query, queryArgs, err := squirrel.Select("donutdb_data").From(tbl.Name).Where(where).ToSql()
	if err != nil {
		return nil, err
	}

	row := tx.QueryRow(query, queryArgs...)
	var oldItemJSON []byte
	err = row.Scan(&oldItemJSON)
	if err == sql.ErrNoRows {
		return nil, donuterr.ResourceNotFoundErr("item not found")
	} else if err != nil {
		return nil, err
	} else {
		err := json.Unmarshal(oldItemJSON, &oldItem)
		if err != nil {
			return nil, fmt.Errorf("corrupt old item in db: %w", err)
		}
	}

	squirrel.Delete(tbl.Name).Where(where).RunWith(tx).Exec()
	shouldRollback = false
	err = tx.Commit()
	if err != nil {
		return nil, fmt.Errorf("commit data err: %w", err)
	}

	return Item(oldItem), nil
}

func hashKeyBytes(keyAttr *dynamodb.AttributeValue, typ string) (string, interface{}, error) {
	var hashBytes []byte
	var key interface{}

	switch typ {
	case "TEXT":
		if keyAttr.S == nil {
			return "", nil, errors.New("invalid type for string key")
		}
		hashBytes = []byte(*keyAttr.S)
		key = *keyAttr.S
	case "REAL":
		if keyAttr.N == nil {
			return "", nil, errors.New("invalid type for numeric key")
		}
		f, err := strconv.ParseFloat(*keyAttr.N, 64)
		if err != nil {
			return "", nil, errors.New("non-numeric value for numeric key")
		}

		hashBytes = make([]byte, 8)
		binary.BigEndian.PutUint64(hashBytes, math.Float64bits(f))

		key = f
	case "BLOB":
		if keyAttr.B == nil {
			return "", nil, errors.New("invalid type for binary key")
		}
		hashBytes = keyAttr.B
		key = keyAttr.B
	default:
		return "", nil, errors.New("unexpected hash key type in database")
	}

	hasher := murmur3.New64()
	hasher.Write(hashBytes)
	return hex.EncodeToString(hasher.Sum(nil)), key, nil
}

func rangeKeyI(keyAttr *dynamodb.AttributeValue, typ string) (interface{}, error) {
	switch typ {
	case "TEXT":
		if keyAttr.S == nil {
			return nil, errors.New("invalid type for string key")
		}
		return *keyAttr.S, nil
	case "REAL":
		if keyAttr.N == nil {
			return nil, errors.New("invalid type for numeric key")
		}
		f, err := strconv.ParseFloat(*keyAttr.N, 64)
		if err != nil {
			return nil, errors.New("non-numeric value for numeric key")
		}

		return f, nil
	case "BLOB":
		if keyAttr.B == nil {
			return nil, errors.New("invalid type for binary key")
		}
		return keyAttr.B, nil
	}
	return nil, errors.New("unexpected hash key type in database")
}

func insertOrReplace(into string) squirrel.InsertBuilder {
	ib := squirrel.InsertBuilder(squirrel.StatementBuilder)
	ib = builder.Set(ib, "StatementKeyword", "INSERT OR REPLACE").(squirrel.InsertBuilder)
	return ib.Into(into)
}
