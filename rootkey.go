package boltdbrootkeystore

import (
	"sync"
	"time"

	bolt "github.com/coreos/bbolt"
	"gopkg.in/macaroon-bakery.v2/bakery"
	"gopkg.in/macaroon-bakery.v2/bakery/dbrootkeystore"
)

var (
	// Some vars which can be overridden for testing.
	clock      dbrootkeystore.Clock
	newBacking = func(s *RootKeys) dbrootkeystore.Backing {
		return backing{s}
	}
)

// Policy holds a store policy for root keys.
type Policy dbrootkeystore.Policy

// RootKeys represents a cache of macaroon root keys.
type RootKeys struct {
	keys *dbrootkeystore.RootKeys

	db     *bolt.DB
	bucket []byte

	// initDBOnce guards initDBErr.
	initDBOnce sync.Once
	initDBErr  error
}

// NewRootKeys creates a RootKeys object that uses the provided bucket within
// the specified db for persisting keys.
func NewRootKeys(db *bolt.DB, bucket []byte, maxCacheSize int) *RootKeys {
	return &RootKeys{
		keys:   dbrootkeystore.NewRootKeys(maxCacheSize, clock),
		db:     db,
		bucket: bucket,
	}
}

// NewStore creates a new RootKeyStore given a store policy.
func (s *RootKeys) NewStore(policy Policy) bakery.RootKeyStore {
	b := newBacking(s)
	return s.keys.NewStore(b, dbrootkeystore.Policy(policy))
}

// backing implements dbrootkeystore.Backing by using bolt as a backing store.
type backing struct {
	keys *RootKeys
}

// GetKey implements dbrootkeystore.Backing.GetKey.
func (b backing) GetKey(id []byte) (dbrootkeystore.RootKey, error) {
	return b.keys.getKey(id)
}

// InsertKey implements dbrootkeystore.Backing.InsertKey.
func (b backing) InsertKey(key dbrootkeystore.RootKey) error {
	return b.keys.insertKey(key)
}

// FindLatestKey implements dbrootkeystore.Backing.FindLatestKey.
func (b backing) FindLatestKey(createdAfter, expiresAfter, expiresBefore time.Time) (dbrootkeystore.RootKey, error) {
	return b.keys.findLatestKey(createdAfter, expiresAfter, expiresBefore)
}
