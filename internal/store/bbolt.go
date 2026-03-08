package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"go.etcd.io/bbolt"

	"github.com/ngaddam369/saga-conductor/internal/saga"
)

var (
	bucketSagas = []byte("sagas")

	// ErrNotFound is returned when a saga ID does not exist in the store.
	ErrNotFound = errors.New("saga not found")

	// ErrAlreadyExists is returned when Create is called with a duplicate ID.
	ErrAlreadyExists = errors.New("saga already exists")
)

// BoltStore is a bbolt-backed implementation of Store.
type BoltStore struct {
	db *bbolt.DB
}

// NewBoltStore opens (or creates) a bbolt database at path and returns a
// ready-to-use BoltStore.
func NewBoltStore(path string) (*BoltStore, error) {
	db, err := bbolt.Open(path, 0o600, nil)
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

func (s *BoltStore) List(_ context.Context, status saga.SagaStatus) ([]*saga.Execution, error) {
	var results []*saga.Execution

	err := s.db.View(func(tx *bbolt.Tx) error {
		return tx.Bucket(bucketSagas).ForEach(func(_, v []byte) error {
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
