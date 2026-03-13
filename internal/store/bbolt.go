package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"

	"go.etcd.io/bbolt"

	"github.com/ngaddam369/saga-conductor/internal/saga"
)

var (
	bucketSagas = []byte("sagas")

	// ErrNotFound is returned when a saga ID does not exist in the store.
	ErrNotFound = errors.New("saga not found")

	// ErrAlreadyExists is returned when Create is called with a duplicate ID.
	ErrAlreadyExists = errors.New("saga already exists")

	// ErrAlreadyRunning is returned by TransitionToRunning when the saga is
	// already RUNNING (concurrent Start calls).
	ErrAlreadyRunning = errors.New("saga is already running")

	// ErrAlreadyCompensating is returned when the saga is mid-rollback.
	ErrAlreadyCompensating = errors.New("saga is already compensating")

	// ErrAlreadyCompleted is returned when the saga has already completed successfully.
	ErrAlreadyCompleted = errors.New("saga is already completed")

	// ErrAlreadyFailed is returned when the saga has already reached a failed terminal state.
	ErrAlreadyFailed = errors.New("saga has already failed")
)

// BoltStore is a bbolt-backed implementation of Store.
type BoltStore struct {
	db *bbolt.DB
}

const defaultLockTimeout = 5 * time.Second

func lockTimeout() time.Duration {
	if v := os.Getenv("DB_LOCK_TIMEOUT_SECONDS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return time.Duration(n) * time.Second
		}
	}
	return defaultLockTimeout
}

// NewBoltStore opens (or creates) a bbolt database at path and returns a
// ready-to-use BoltStore. It fails fast if the file lock cannot be acquired
// within DB_LOCK_TIMEOUT_SECONDS (default 5s).
func NewBoltStore(path string) (*BoltStore, error) {
	db, err := bbolt.Open(path, 0o600, &bbolt.Options{Timeout: lockTimeout()})
	if err != nil {
		return nil, fmt.Errorf("open bbolt db: %w", err)
	}

	if err := db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(bucketSagas)
		return err
	}); err != nil {
		return nil, fmt.Errorf("create bucket: %w", err)
	}

	return &BoltStore{db: db}, nil
}

// Close releases the database file lock. Must be called on shutdown.
func (s *BoltStore) Close() error {
	return s.db.Close()
}

func (s *BoltStore) Create(_ context.Context, exec *saga.Execution) error {
	return s.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketSagas)
		key := []byte(exec.ID)

		if b.Get(key) != nil {
			return ErrAlreadyExists
		}

		data, err := json.Marshal(exec)
		if err != nil {
			return fmt.Errorf("marshal saga: %w", err)
		}
		return b.Put(key, data)
	})
}

func (s *BoltStore) Get(_ context.Context, id string) (*saga.Execution, error) {
	var exec saga.Execution
	err := s.db.View(func(tx *bbolt.Tx) error {
		data := tx.Bucket(bucketSagas).Get([]byte(id))
		if data == nil {
			return ErrNotFound
		}
		return json.Unmarshal(data, &exec)
	})
	if err != nil {
		return nil, err
	}
	return &exec, nil
}

func (s *BoltStore) Update(_ context.Context, exec *saga.Execution) error {
	return s.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketSagas)
		key := []byte(exec.ID)

		if b.Get(key) == nil {
			return ErrNotFound
		}

		data, err := json.Marshal(exec)
		if err != nil {
			return fmt.Errorf("marshal saga: %w", err)
		}
		return b.Put(key, data)
	})
}

func (s *BoltStore) TransitionToRunning(ctx context.Context, id string, startedAt time.Time) (*saga.Execution, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var exec saga.Execution
	err := s.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketSagas)
		data := b.Get([]byte(id))
		if data == nil {
			return ErrNotFound
		}
		if err := json.Unmarshal(data, &exec); err != nil {
			return fmt.Errorf("unmarshal saga: %w", err)
		}
		switch exec.Status {
		case saga.SagaStatusPending:
			// allowed — fall through to write
		case saga.SagaStatusRunning:
			return ErrAlreadyRunning
		case saga.SagaStatusCompensating:
			return ErrAlreadyCompensating
		case saga.SagaStatusCompleted:
			return ErrAlreadyCompleted
		case saga.SagaStatusFailed:
			return ErrAlreadyFailed
		default:
			return fmt.Errorf("saga is in unexpected status %q", exec.Status)
		}
		exec.Status = saga.SagaStatusRunning
		exec.StartedAt = &startedAt
		updated, err := json.Marshal(&exec)
		if err != nil {
			return fmt.Errorf("marshal saga: %w", err)
		}
		return b.Put([]byte(id), updated)
	})
	if err != nil {
		return nil, err
	}
	return &exec, nil
}

func (s *BoltStore) Delete(_ context.Context, id string) error {
	return s.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketSagas)
		key := []byte(id)
		if b.Get(key) == nil {
			return ErrNotFound
		}
		return b.Delete(key)
	})
}

func (s *BoltStore) Ping(_ context.Context) error {
	return s.db.View(func(tx *bbolt.Tx) error {
		if tx.Bucket(bucketSagas) == nil {
			return fmt.Errorf("sagas bucket missing")
		}
		return nil
	})
}

func (s *BoltStore) List(ctx context.Context, status saga.SagaStatus) ([]*saga.Execution, error) {
	var results []*saga.Execution

	err := s.db.View(func(tx *bbolt.Tx) error {
		return tx.Bucket(bucketSagas).ForEach(func(_, v []byte) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			var exec saga.Execution
			if err := json.Unmarshal(v, &exec); err != nil {
				return fmt.Errorf("unmarshal saga: %w", err)
			}
			if status == "" || exec.Status == status {
				results = append(results, &exec)
			}
			return nil
		})
	})
	return results, err
}
