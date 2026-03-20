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
	bucketSagas           = []byte("sagas")
	bucketIdempotencyKeys = []byte("idempotency_keys")

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

	// ErrAlreadyAborted is returned when AbortSaga is called on a saga that has
	// already been aborted.
	ErrAlreadyAborted = errors.New("saga has already been aborted")
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
		for _, b := range [][]byte{bucketSagas, bucketIdempotencyKeys} {
			if _, err := tx.CreateBucketIfNotExists(b); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return nil, fmt.Errorf("create buckets: %w", err)
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

// idempotencyRecord is the value stored in bucketIdempotencyKeys.
type idempotencyRecord struct {
	SagaID    string    `json:"saga_id"`
	ExpiresAt time.Time `json:"expires_at"`
}

func (s *BoltStore) GetOrCreateWithKey(_ context.Context, key string, expiresAt time.Time, exec *saga.Execution) (*saga.Execution, error) {
	var result saga.Execution
	err := s.db.Update(func(tx *bbolt.Tx) error {
		kb := tx.Bucket(bucketIdempotencyKeys)
		sb := tx.Bucket(bucketSagas)

		// Check for a live idempotency record.
		if raw := kb.Get([]byte(key)); raw != nil {
			var rec idempotencyRecord
			if err := json.Unmarshal(raw, &rec); err == nil && time.Now().Before(rec.ExpiresAt) {
				// Key still valid — return the existing saga.
				data := sb.Get([]byte(rec.SagaID))
				if data != nil {
					return json.Unmarshal(data, &result)
				}
				// Saga was deleted (e.g. pruned) — fall through to recreate.
			}
			// Key expired or record corrupt — overwrite below.
		}

		// Create the saga.
		if sb.Get([]byte(exec.ID)) != nil {
			return ErrAlreadyExists
		}
		data, err := json.Marshal(exec)
		if err != nil {
			return fmt.Errorf("marshal saga: %w", err)
		}
		if err := sb.Put([]byte(exec.ID), data); err != nil {
			return err
		}

		// Record the idempotency key.
		rec := idempotencyRecord{SagaID: exec.ID, ExpiresAt: expiresAt}
		recData, err := json.Marshal(rec)
		if err != nil {
			return fmt.Errorf("marshal idempotency record: %w", err)
		}
		if err := kb.Put([]byte(key), recData); err != nil {
			return err
		}

		result = *exec
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &result, nil
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
		case saga.SagaStatusAborted:
			return ErrAlreadyAborted
		case saga.SagaStatusCompensationFailed:
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

func (s *BoltStore) List(ctx context.Context, status saga.SagaStatus, pageSize int, pageToken string) ([]*saga.Execution, string, error) {
	var (
		results   []*saga.Execution
		nextToken string
	)

	err := s.db.View(func(tx *bbolt.Tx) error {
		c := tx.Bucket(bucketSagas).Cursor()

		// Position the cursor. pageToken is the ID of the last item returned
		// on the previous page; Seek lands on it (or the next key if deleted),
		// then we skip it with Next so we don't re-deliver it.
		var k, v []byte
		if pageToken != "" {
			k, v = c.Seek([]byte(pageToken))
			if k != nil && string(k) == pageToken {
				k, v = c.Next() // skip the token itself
			}
		} else {
			k, v = c.First()
		}

		for ; k != nil; k, v = c.Next() {
			if err := ctx.Err(); err != nil {
				return err
			}
			var exec saga.Execution
			if err := json.Unmarshal(v, &exec); err != nil {
				return fmt.Errorf("unmarshal saga: %w", err)
			}
			if status != "" && exec.Status != status {
				continue
			}
			results = append(results, &exec)
			if pageSize > 0 && len(results) == pageSize {
				// Return a token so callers can fetch the next page. If no
				// further matching items exist the caller will receive an empty
				// page with no token, signalling end-of-results.
				nextToken = exec.ID
				break
			}
		}
		return nil
	})
	return results, nextToken, err
}
