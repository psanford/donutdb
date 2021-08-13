package donutdb

import (
	"errors"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/psanford/sqlite3vfs"
)

var deadlineDuration = 2 * time.Second
var renewDuration = 750 * time.Millisecond

type globalLockManager struct {
	db        *dynamodb.DynamoDB
	table     string
	lockName  string
	lockLevel sqlite3vfs.LockType
	ownerID   string

	startTicker   chan startTickerMsg
	stopTicker    chan struct{}
	unlockEvent   chan struct{}
	heartbeatDone chan struct{}

	err error
}

func newGlobalLockManger(db *dynamodb.DynamoDB, table, lockName, owner string) *globalLockManager {
	lm := &globalLockManager{
		db:       db,
		table:    table,
		lockName: lockName,
		ownerID:  owner,

		startTicker:   make(chan startTickerMsg),
		stopTicker:    make(chan struct{}),
		unlockEvent:   make(chan struct{}),
		heartbeatDone: make(chan struct{}),
	}

	go lm.heartbeatLoop()

	return lm
}

func (m *globalLockManager) lock(elock sqlite3vfs.LockType) error {
	if m.err != nil {
		return m.err
	}

	if elock < m.lockLevel {
		return nil
	}

	if m.lockLevel > sqlite3vfs.LockNone {
		// we already hold the lock, update the internal state and return
		m.lockLevel = elock
		return nil
	}

	handleUpdateItemResult := func(deadline string, err error) error {
		if err != nil {
			if _, match := err.(*dynamodb.ConditionalCheckFailedException); match {
				// someone else beat us to the lock
				return sqlite3vfs.BusyError
			}
			// we hit some other error
			return err
		}

		// we got the lock!
		m.lockLevel = elock
		select {
		case m.startTicker <- startTickerMsg{prevDeadline: deadline}:
		case <-time.After(10 * time.Second):
			panic("startTicker msg send blocked for more than 10s, something is wrong")
		}
		return nil
	}

	item, err := m.db.GetItem(&dynamodb.GetItemInput{
		TableName:       &m.table,
		ConsistentRead:  aws.Bool(true),
		AttributesToGet: []*string{aws.String("owner_id"), aws.String("deadline_us")},
		Key: map[string]*dynamodb.AttributeValue{
			hKey: {
				S: &m.lockName,
			},
			rKey: {
				N: aws.String("0"),
			},
		},
	})

	if err != nil {
		return err
	}

	oldOwner := item.Item["owner_id"]
	oldDeadlineUsS, exists := item.Item["deadline_us"]
	if !exists {
		// no one holds the lock, lets try to get it

		deadline := time.Now().Add(deadlineDuration)
		deadlineUsS := strconv.FormatInt(unixMicro(deadline), 10)

		_, err = m.db.PutItem(&dynamodb.PutItemInput{
			TableName:           &m.table,
			ConditionExpression: aws.String("attribute_not_exists(deadline_us)"),
			Item: map[string]*dynamodb.AttributeValue{
				hKey: {
					S: &m.lockName,
				},
				rKey: {
					N: aws.String("0"),
				},
				"owner_id": {
					S: &m.ownerID,
				},
				"deadline_us": {
					N: &deadlineUsS,
				},
			},
		})

		return handleUpdateItemResult(deadlineUsS, err)
	}

	oldDeadlineUs, err := strconv.ParseInt(*oldDeadlineUsS.N, 10, 64)
	if err != nil {
		return err
	}

	if unixMicro(time.Now()) > oldDeadlineUs {
		// the existing lock has expired, lets try to take it

		deadline := time.Now().Add(deadlineDuration)
		deadlineUsS := strconv.FormatInt(unixMicro(deadline), 10)

		_, err = m.db.PutItem(&dynamodb.PutItemInput{
			TableName:           &m.table,
			ConditionExpression: aws.String("deadline_us = :dus AND owner_id = :own"),
			ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
				":dus": {
					N: oldDeadlineUsS.N,
				},
				":own": {
					S: oldOwner.S,
				},
			},
			Item: map[string]*dynamodb.AttributeValue{
				hKey: {
					S: &m.lockName,
				},
				rKey: {
					N: aws.String("0"),
				},
				"owner_id": {
					S: &m.ownerID,
				},
				"deadline_us": {
					N: &deadlineUsS,
				},
			},
		})

		return handleUpdateItemResult(deadlineUsS, err)
	}

	// someone else holds the lock
	return sqlite3vfs.BusyError
}

func (m *globalLockManager) unlock(elock sqlite3vfs.LockType) error {
	if m.err != nil {
		return m.err
	}

	if elock > sqlite3vfs.LockShared {
		panic(fmt.Sprintf("Invalid unlock request to level %s", elock))
	}

	if m.lockLevel < elock {
		panic("Cannot unlock to a level > current lock level")
	}

	if elock == m.lockLevel {
		return nil
	}

	if elock == sqlite3vfs.LockShared {
		m.lockLevel = sqlite3vfs.LockShared
		return nil
	}

	m.lockLevel = sqlite3vfs.LockNone

	select {
	case m.stopTicker <- struct{}{}:
	case <-time.After(10 * time.Second):
		panic("stopTicker msg send blocked for more than 10s, something is wrong")
	}

	select {
	case <-m.unlockEvent:
	case <-time.After(10 * time.Second):
		panic("unlockEvent waited for more than 10s, something is wrong")
	}

	return nil
}

func (m *globalLockManager) level() sqlite3vfs.LockType {
	return m.lockLevel
}

func (m *globalLockManager) checkReservedLock() (bool, error) {
	if m.lockLevel > sqlite3vfs.LockNone {
		// we hold a lock
		return true, nil
	}

	item, err := m.db.GetItem(&dynamodb.GetItemInput{
		TableName:       &m.table,
		ConsistentRead:  aws.Bool(true),
		AttributesToGet: []*string{aws.String("owner_id"), aws.String("deadline_us")},
		Key: map[string]*dynamodb.AttributeValue{
			hKey: {
				S: &m.lockName,
			},
			rKey: {
				N: aws.String("0"),
			},
		},
	})
	if err != nil {
		return false, err
	}

	deadlineUsS, exists := item.Item["deadline_us"]
	if !exists {
		// no lock exists
		return false, nil
	}

	deadlineUs, err := strconv.ParseInt(*deadlineUsS.N, 10, 64)
	if err != nil {
		return false, err
	}

	if unixMicro(time.Now()) < deadlineUs {
		// the existing lock is still active
		return true, nil
	}

	return false, nil
}

func (m *globalLockManager) close() error {
	close(m.stopTicker)
	<-m.heartbeatDone

	retErr := m.err
	m.err = errors.New("lock manager closed")
	return retErr
}

func (m *globalLockManager) heartbeatLoop() {
	ticker := time.NewTicker(renewDuration)
	ticker.Stop()

	defer close(m.heartbeatDone)

	var (
		running        bool
		prevDeadlineUs string
	)

	for {
		select {
		case startMsg := <-m.startTicker:
			if running {
				panic("Got startTicker event for a lock we already held")
			}

			running = true
			prevDeadlineUs = startMsg.prevDeadline
			ticker.Reset(renewDuration)
		case _, ok := <-m.stopTicker:
			if !running && ok {
				// we should only get normal stopTicker events when we
				// actually hold a lock. This doesn't include close events
				panic("Got stopTicker event but we don't hold a lock")
			}

			// we might not be running if this was a close event
			if running {
				_, err := m.db.DeleteItem(&dynamodb.DeleteItemInput{
					TableName:           &m.table,
					ConditionExpression: aws.String("deadline_us = :dus AND owner_id = :own"),
					ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
						":dus": {
							N: &prevDeadlineUs,
						},
						":own": {
							S: &m.ownerID,
						},
					},
					Key: map[string]*dynamodb.AttributeValue{
						hKey: {
							S: &m.lockName,
						},
						rKey: {
							N: aws.String("0"),
						},
					},
				})
				if err != nil {
					m.err = err
				}

				if ok {
					m.unlockEvent <- struct{}{}
				}
			}

			running = false
			ticker.Stop()

			if !ok {
				// closed chan means we are shutting down
				return
			}
		case <-ticker.C:
			if !running {
				// we could get a stray tick since we don't attempt to drain
				// the ticker chan, so ignore it if we're not in the running state
				continue
			}

			deadline := time.Now().Add(deadlineDuration)
			deadlineUsS := strconv.FormatInt(unixMicro(deadline), 10)

			_, err := m.db.PutItem(&dynamodb.PutItemInput{
				TableName:           &m.table,
				ConditionExpression: aws.String("deadline_us = :dus AND owner_id = :own"),
				ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
					":dus": {
						N: &prevDeadlineUs,
					},
					":own": {
						S: &m.ownerID,
					},
				},
				Item: map[string]*dynamodb.AttributeValue{
					hKey: {
						S: &m.lockName,
					},
					rKey: {
						N: aws.String("0"),
					},
					"owner_id": {
						S: &m.ownerID,
					},
					"deadline_us": {
						N: &deadlineUsS,
					},
				},
			})

			if err != nil {
				if _, match := err.(*dynamodb.ConditionalCheckFailedException); match {
					panic("lost lock while heartbeating!")
				}
				// maybe there was a transient error that we'll recover from on the next tick
				log.Printf("Error heartbeating: %s", err)
			}

			prevDeadlineUs = deadlineUsS
		}
	}
}

type startTickerMsg struct {
	prevDeadline string
}

// remove once minimum supported version is 1.17 with .UnixMicro()
func unixMicro(t time.Time) int64 {
	return t.UnixNano() / 1e3
}
