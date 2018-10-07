package boltdbrootkeystore

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"time"

	bolt "github.com/coreos/bbolt"
	errgo "gopkg.in/errgo.v1"
	"gopkg.in/macaroon-bakery.v2/bakery"
	"gopkg.in/macaroon-bakery.v2/bakery/dbrootkeystore"
)

var (
	keyBucket = []byte("keys")     // bucket that stores keyID/key pairs
	seqBucket = []byte("sequence") // bucket that stores index/keyID pairs
)

// initDB makes sure that the database was initialized. Subsequent calls to
// initDB will return the same error as the first call.
func (s *RootKeys) initDB() error {
	s.initDBOnce.Do(func() {
		// Create buckets if they don't exist.
		s.initDBErr = s.db.Update(func(tx *bolt.Tx) error {
			b, err := tx.CreateBucketIfNotExists(s.bucket)
			if err != nil {
				return err
			}
			_, err = b.CreateBucketIfNotExists(keyBucket)
			if err != nil {
				return err
			}
			_, err = b.CreateBucketIfNotExists(seqBucket)
			if err != nil {
				return err
			}
			return err
		})
	})
	if s.initDBErr != nil {
		return errgo.Notef(s.initDBErr, "cannot initialize database")
	}
	return nil
}

// getKey gets the key with the given id from the
// backing store. If the key is not found, it should
// return an error with a bakery.ErrNotFound cause.
func (s *RootKeys) getKey(id []byte) (key dbrootkeystore.RootKey, err error) {
	if err := s.initDB(); err != nil {
		return dbrootkeystore.RootKey{}, errgo.Mask(err)
	}
	err = s.db.View(func(tx *bolt.Tx) error {
		// Retrieve key from bucket.
		keyBytes := tx.Bucket(s.bucket).Bucket(keyBucket).Get(id)
		if keyBytes == nil {
			return bakery.ErrNotFound
		}
		// Decrypt key.
		keyBytes, err = s.decrypt(keyBytes)
		if err != nil {
			return err
		}
		// Decode key.
		decoder := gob.NewDecoder(bytes.NewReader(keyBytes))
		if err := decoder.Decode(&key); err != nil {
			return err
		}
		return nil
	})
	return
}

// findLatestKey returns the most recently created root key k
// such that all of the following conditions hold:
//
// 	k.Created >= createdAfter
//	k.Expires >= expiresAfter
//	k.Expires <= expiresBefore
//
// If no such key was found, the zero root key should be returned
// with a nil error.
func (s *RootKeys) findLatestKey(createdAfter, expiresAfter, expiresBefore time.Time) (key dbrootkeystore.RootKey, err error) {
	if err := s.initDB(); err != nil {
		return dbrootkeystore.RootKey{}, errgo.Mask(err)
	}
	err = s.db.View(func(tx *bolt.Tx) error {
		keyBucket := tx.Bucket(s.bucket).Bucket(keyBucket)
		seqBucket := tx.Bucket(s.bucket).Bucket(seqBucket)

		// Loop over all the keys starting from the most recent one until
		// either a key is found that satisfies the conditions, or we encounter
		// a key that violates the 'Created' condition.
		c := seqBucket.Cursor()
		for seqBytes, keyID := c.Last(); seqBytes != nil; seqBytes, keyID = c.Prev() {
			// Get the key.
			keyBytes := keyBucket.Get(keyID)
			if keyBytes == nil {
				return errors.New("Key should never be in seqBucket but not in the keyBucket")
			}
			// Decrypt key.
			keyBytes, err = s.decrypt(keyBytes)
			if err != nil {
				return err
			}
			// Decode the key.
			decoder := gob.NewDecoder(bytes.NewReader(keyBytes))
			if err = decoder.Decode(&key); err != nil {
				return err
			}
			// Check if the key meets the requirements.
			if key.Expires.Before(expiresAfter) {
				continue
			}
			if key.Expires.After(expiresBefore) {
				continue
			}
			if key.Created.Before(createdAfter) {
				break
			}
			// All conditions are met. We found the key.
			return nil
		}
		// If no key was found, we return the empty key.
		key = dbrootkeystore.RootKey{}
		return nil
	})
	return
}

// InsertKey inserts the given root key into the backing store.
// It may return an error if the id or key already exist.
func (s *RootKeys) insertKey(key dbrootkeystore.RootKey) error {
	if err := s.initDB(); err != nil {
		return errgo.Mask(err)
	}
	return s.db.Update(func(tx *bolt.Tx) error {
		keyBucket := tx.Bucket(s.bucket).Bucket(keyBucket)
		seqBucket := tx.Bucket(s.bucket).Bucket(seqBucket)

		// When we insert a new key we check all old keys for expired ones
		// first and delete them.
		// TODO Pruning the database at every insert is not very fast. This
		// could be done periodically instead.
		c := seqBucket.Cursor()
		for seqBytes, keyID := c.First(); seqBytes != nil; seqBytes, keyID = c.Next() {
			// Get the key.
			keyBytes := keyBucket.Get(keyID)
			if keyBytes == nil {
				return errors.New("Key should never be in seqBucket but not in the keyBucket")
			}
			// Decrypt key.
			keyBytes, err := s.decrypt(keyBytes)
			if err != nil {
				return err
			}
			// Decode the key.
			var oldKey dbrootkeystore.RootKey
			decoder := gob.NewDecoder(bytes.NewReader(keyBytes))
			if err := decoder.Decode(&oldKey); err != nil {
				return err
			}
			if oldKey.Expires.After(time.Now()) {
				continue
			}
			// Found a key to delete.
			if err := seqBucket.Delete(seqBytes); err != nil {
				return err
			}
			if err := keyBucket.Delete(keyID); err != nil {
				return err
			}
		}

		// Encode the key.
		buf := bytes.NewBuffer(nil)
		enc := gob.NewEncoder(buf)
		if err := enc.Encode(key); err != nil {
			return err
		}
		// Encrypt key.
		keyBytes, err := s.encrypt(buf.Bytes())
		if err != nil {
			return err
		}
		// Store the key in the key bucket.
		if err := keyBucket.Put(key.Id, keyBytes); err != nil {
			return err
		}
		// Store the id of the key in the sequence bucket.
		seq, err := seqBucket.NextSequence()
		if err != nil {
			return err
		}
		seqBytes := make([]byte, 8)
		binary.LittleEndian.PutUint64(seqBytes, seq)
		if err := seqBucket.Put(seqBytes, key.Id); err != nil {
			return err
		}
		return nil
	})
}
