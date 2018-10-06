# boltdbrootkeystore

An implementation of bakery.RootKeyStore that uses boltdb as a persistent store.

This implementation uses the dbrootkeystore.RootKeys of the bakery for generating and caching chunks by implementing the dbrootkeystore.Backing interface with boltb as a backend. To improve performance, the database uses two buckets. The `keyBucket` stores root keys by id and the `seqBucket` one stores the key ids using an auto-incremented index. For that reason it is assumed that keys are always inserted in the same order as they are created. 

TODOS:
- [ ] Add optional encryption of keys in database
- [ ] Maybe replace prune on insert with periodic pruning
- [ ] Use binary-search in `FindLatestKey` 
