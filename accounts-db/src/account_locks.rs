#[cfg(feature = "dev-context-only-utils")]
use qualifier_attr::qualifiers;
use {
    ahash::{AHashMap, AHashSet},
    dashmap::DashMap,
    smallvec::SmallVec,
    solana_message::AccountKeys,
    solana_pubkey::Pubkey,
    solana_transaction::sanitized::MAX_TX_ACCOUNT_LOCKS,
    solana_transaction_error::{TransactionError, TransactionResult},
    std::{cell::RefCell, collections::hash_map},
};

#[derive(Debug, Default)]
pub struct AccountLocks {
    write_locks: AHashMap<Pubkey, u64>,
    readonly_locks: AHashMap<Pubkey, u64>,
}

impl AccountLocks {
    /// Lock accounts for all transactions in a batch which don't conflict
    /// with existing locks. Returns a vector of `TransactionResult` indicating
    /// success or failure for each transaction in the batch.
    pub fn try_lock_transaction_batch<'a>(
        &mut self,
        mut validated_batch_keys: Vec<
            TransactionResult<impl Iterator<Item = (&'a Pubkey, bool)> + Clone>,
        >,
    ) -> Vec<TransactionResult<()>> {
        validated_batch_keys.iter_mut().for_each(|validated_keys| {
            if let Ok(keys) = validated_keys.as_ref() {
                if let Err(e) = self.can_lock_accounts(keys.clone()) {
                    *validated_keys = Err(e);
                }
            }
        });

        validated_batch_keys
            .into_iter()
            .map(|available_keys| available_keys.map(|keys| self.lock_accounts(keys)))
            .collect()
    }

    /// Unlock the account keys in `keys` after a transaction.
    /// The bool in the tuple indicates if the account is writable.
    /// In debug-mode this function will panic if an attempt is made to unlock
    /// an account that wasn't locked in the way requested.
    pub fn unlock_accounts<'a>(&mut self, keys: impl Iterator<Item = (&'a Pubkey, bool)>) {
        for (k, writable) in keys {
            if writable {
                self.unlock_write(k);
            } else {
                self.unlock_readonly(k);
            }
        }
    }

    fn can_lock_accounts<'a>(
        &self,
        keys: impl Iterator<Item = (&'a Pubkey, bool)>,
    ) -> TransactionResult<()> {
        for (key, writable) in keys {
            if writable {
                if !self.can_write_lock(key) {
                    return Err(TransactionError::AccountInUse);
                }
            } else if !self.can_read_lock(key) {
                return Err(TransactionError::AccountInUse);
            }
        }

        Ok(())
    }

    fn lock_accounts<'a>(&mut self, keys: impl Iterator<Item = (&'a Pubkey, bool)>) {
        for (key, writable) in keys {
            if writable {
                self.lock_write(key);
            } else {
                self.lock_readonly(key);
            }
        }
    }

    #[cfg_attr(feature = "dev-context-only-utils", qualifiers(pub))]
    fn is_locked_readonly(&self, key: &Pubkey) -> bool {
        self.readonly_locks.get(key).is_some_and(|count| *count > 0)
    }

    #[cfg_attr(feature = "dev-context-only-utils", qualifiers(pub))]
    fn is_locked_write(&self, key: &Pubkey) -> bool {
        self.write_locks.get(key).is_some_and(|count| *count > 0)
    }

    fn can_read_lock(&self, key: &Pubkey) -> bool {
        // If the key is not write-locked, it can be read-locked
        !self.is_locked_write(key)
    }

    fn can_write_lock(&self, key: &Pubkey) -> bool {
        // If the key is not read-locked or write-locked, it can be write-locked
        !self.is_locked_readonly(key) && !self.is_locked_write(key)
    }

    fn lock_readonly(&mut self, key: &Pubkey) {
        *self.readonly_locks.entry(*key).or_default() += 1;
    }

    fn lock_write(&mut self, key: &Pubkey) {
        *self.write_locks.entry(*key).or_default() += 1;
    }

    fn unlock_readonly(&mut self, key: &Pubkey) {
        if let hash_map::Entry::Occupied(mut occupied_entry) = self.readonly_locks.entry(*key) {
            let count = occupied_entry.get_mut();
            *count -= 1;
            if *count == 0 {
                occupied_entry.remove_entry();
            }
        } else {
            debug_assert!(
                false,
                "Attempted to remove a read-lock for a key that wasn't read-locked"
            );
        }
    }

    fn unlock_write(&mut self, key: &Pubkey) {
        if let hash_map::Entry::Occupied(mut occupied_entry) = self.write_locks.entry(*key) {
            let count = occupied_entry.get_mut();
            *count -= 1;
            if *count == 0 {
                occupied_entry.remove_entry();
            }
        } else {
            debug_assert!(
                false,
                "Attempted to remove a write-lock for a key that wasn't write-locked"
            );
        }
    }
}

/// Lock state for a single account: tracks separate read and write lock counts.
/// Under SIMD83, a key may have both read and write locks from the same batch.
#[derive(Debug, Default)]
struct LockState {
    readers: u64,
    writers: u64,
}

/// DashMap-based concurrent account locks.
/// Allows lock/unlock from multiple threads without a global Mutex.
/// Uses per-key sharding for high concurrency.
#[derive(Debug, Default)]
pub struct ConcurrentAccountLocks {
    locks: DashMap<Pubkey, LockState>,
}

impl ConcurrentAccountLocks {
    /// Try to lock accounts for a single transaction.
    /// Uses atomic entry-based operations with rollback on conflict.
    pub fn try_lock_accounts<'a>(
        &self,
        keys: impl Iterator<Item = (&'a Pubkey, bool)> + Clone,
    ) -> TransactionResult<()> {
        // Collect keys so we can rollback on failure.
        let keys_vec: SmallVec<[(&Pubkey, bool); 32]> = keys.collect();
        let mut locked_count = 0usize;

        for &(key, writable) in &keys_vec {
            let success = if writable {
                self.try_write_lock(key)
            } else {
                self.try_read_lock(key)
            };

            if !success {
                // Rollback all locks acquired so far
                for &(rkey, rwritable) in &keys_vec[..locked_count] {
                    if rwritable {
                        self.unlock_write(rkey);
                    } else {
                        self.unlock_read(rkey);
                    }
                }
                return Err(TransactionError::AccountInUse);
            }
            locked_count += 1;
        }
        Ok(())
    }

    /// Lock accounts for all transactions in a batch (SIMD83).
    /// Two-phase: first check all txs against existing locks (NOT against each other),
    /// then lock all that passed. Intra-batch conflicts are intentionally allowed.
    pub fn try_lock_transaction_batch<'a>(
        &self,
        mut validated_batch_keys: Vec<
            TransactionResult<impl Iterator<Item = (&'a Pubkey, bool)> + Clone>,
        >,
    ) -> Vec<TransactionResult<()>> {
        // Phase 1: Check each tx against existing locks only (not against each other)
        validated_batch_keys.iter_mut().for_each(|validated_keys| {
            if let Ok(keys) = validated_keys.as_ref() {
                for (key, writable) in keys.clone() {
                    let conflict = if writable {
                        // Write conflicts with any existing lock
                        self.locks.get(key).is_some_and(|v| v.readers > 0 || v.writers > 0)
                    } else {
                        // Read only conflicts with existing write lock
                        self.locks.get(key).is_some_and(|v| v.writers > 0)
                    };
                    if conflict {
                        *validated_keys = Err(TransactionError::AccountInUse);
                        break;
                    }
                }
            }
        });

        // Phase 2: Lock all txs that passed validation
        // Under SIMD83, intra-batch conflicts are allowed, so multiple
        // writers in the same batch increment the writer count.
        validated_batch_keys
            .into_iter()
            .map(|result| {
                result.map(|keys| {
                    for (key, writable) in keys {
                        let mut entry = self.locks.entry(*key).or_default();
                        if writable {
                            entry.writers += 1;
                        } else {
                            entry.readers += 1;
                        }
                    }
                })
            })
            .collect()
    }

    /// Unlock accounts after a transaction completes.
    pub fn unlock_accounts<'a>(&self, keys: impl Iterator<Item = (&'a Pubkey, bool)>) {
        for (key, writable) in keys {
            if writable {
                self.unlock_write(key);
            } else {
                self.unlock_read(key);
            }
        }
    }

    fn try_read_lock(&self, key: &Pubkey) -> bool {
        let mut entry = self.locks.entry(*key).or_default();
        // Read lock fails if account is write-locked
        if entry.writers == 0 {
            entry.readers += 1;
            true
        } else {
            if entry.readers == 0 && entry.writers == 0 {
                drop(entry);
                self.locks.remove(key);
            }
            false
        }
    }

    fn try_write_lock(&self, key: &Pubkey) -> bool {
        let mut entry = self.locks.entry(*key).or_default();
        // Write lock fails if account is read-locked or write-locked
        if entry.readers == 0 && entry.writers == 0 {
            entry.writers = 1;
            true
        } else {
            false
        }
    }

    fn unlock_read(&self, key: &Pubkey) {
        if let Some(mut entry) = self.locks.get_mut(key) {
            debug_assert!(entry.readers > 0, "unlock_read on non-read-locked key");
            entry.readers -= 1;
            if entry.readers == 0 && entry.writers == 0 {
                drop(entry);
                self.locks.remove(key);
            }
        } else {
            debug_assert!(false, "unlock_read on non-existent key");
        }
    }

    fn unlock_write(&self, key: &Pubkey) {
        if let Some(mut entry) = self.locks.get_mut(key) {
            debug_assert!(entry.writers > 0, "unlock_write on non-write-locked key");
            entry.writers -= 1;
            if entry.readers == 0 && entry.writers == 0 {
                drop(entry);
                self.locks.remove(key);
            }
        } else {
            debug_assert!(false, "unlock_write on non-existent key");
        }
    }

    #[cfg(feature = "dev-context-only-utils")]
    pub fn is_locked_readonly(&self, key: &Pubkey) -> bool {
        self.locks.get(key).is_some_and(|v| v.readers > 0)
    }

    #[cfg(feature = "dev-context-only-utils")]
    pub fn is_locked_write(&self, key: &Pubkey) -> bool {
        self.locks.get(key).is_some_and(|v| v.writers > 0)
    }
}

/// Validate account locks before locking.
pub fn validate_account_locks(
    account_keys: AccountKeys,
    tx_account_lock_limit: usize,
) -> TransactionResult<()> {
    if account_keys.len() > tx_account_lock_limit {
        Err(TransactionError::TooManyAccountLocks)
    } else if has_duplicates(account_keys) {
        Err(TransactionError::AccountLoadedTwice)
    } else {
        Ok(())
    }
}

thread_local! {
    static HAS_DUPLICATES_SET: RefCell<AHashSet<Pubkey>> = RefCell::new(AHashSet::with_capacity(MAX_TX_ACCOUNT_LOCKS));
}

/// Check for duplicate account keys.
fn has_duplicates(account_keys: AccountKeys) -> bool {
    // Benchmarking has shown that for sets of 32 or more keys, it is faster to
    // use a HashSet to check for duplicates.
    // For smaller sets a brute-force O(n^2) check seems to be faster.
    const USE_ACCOUNT_LOCK_SET_SIZE: usize = 32;
    if account_keys.len() >= USE_ACCOUNT_LOCK_SET_SIZE {
        HAS_DUPLICATES_SET.with_borrow_mut(|set| {
            let has_duplicates = account_keys.iter().any(|key| !set.insert(*key));
            set.clear();
            has_duplicates
        })
    } else {
        for (idx, key) in account_keys.iter().enumerate() {
            for jdx in idx + 1..account_keys.len() {
                if key == &account_keys[jdx] {
                    return true;
                }
            }
        }
        false
    }
}

#[cfg(test)]
mod tests {
    use {super::*, solana_message::v0::LoadedAddresses};

    #[test]
    fn test_validate_account_locks_valid_no_dynamic() {
        let static_keys = &[Pubkey::new_unique(), Pubkey::new_unique()];
        let account_keys = AccountKeys::new(static_keys, None);
        assert!(validate_account_locks(account_keys, MAX_TX_ACCOUNT_LOCKS).is_ok());
    }

    #[test]
    fn test_validate_account_locks_too_many_no_dynamic() {
        let static_keys = &[Pubkey::new_unique(), Pubkey::new_unique()];
        let account_keys = AccountKeys::new(static_keys, None);
        assert_eq!(
            validate_account_locks(account_keys, 1),
            Err(TransactionError::TooManyAccountLocks)
        );
    }

    #[test]
    fn test_validate_account_locks_duplicate_no_dynamic() {
        let duplicate_key = Pubkey::new_unique();
        let static_keys = &[duplicate_key, Pubkey::new_unique(), duplicate_key];
        let account_keys = AccountKeys::new(static_keys, None);
        assert_eq!(
            validate_account_locks(account_keys, MAX_TX_ACCOUNT_LOCKS),
            Err(TransactionError::AccountLoadedTwice)
        );
    }

    #[test]
    fn test_validate_account_locks_valid_dynamic() {
        let static_keys = &[Pubkey::new_unique(), Pubkey::new_unique()];
        let dynamic_keys = LoadedAddresses {
            writable: vec![Pubkey::new_unique()],
            readonly: vec![Pubkey::new_unique()],
        };
        let account_keys = AccountKeys::new(static_keys, Some(&dynamic_keys));
        assert!(validate_account_locks(account_keys, MAX_TX_ACCOUNT_LOCKS).is_ok());
    }

    #[test]
    fn test_validate_account_locks_too_many_dynamic() {
        let static_keys = &[Pubkey::new_unique()];
        let dynamic_keys = LoadedAddresses {
            writable: vec![Pubkey::new_unique()],
            readonly: vec![Pubkey::new_unique()],
        };
        let account_keys = AccountKeys::new(static_keys, Some(&dynamic_keys));
        assert_eq!(
            validate_account_locks(account_keys, 2),
            Err(TransactionError::TooManyAccountLocks)
        );
    }

    #[test]
    fn test_validate_account_locks_duplicate_dynamic() {
        let duplicate_key = Pubkey::new_unique();
        let static_keys = &[duplicate_key];
        let dynamic_keys = LoadedAddresses {
            writable: vec![Pubkey::new_unique()],
            readonly: vec![duplicate_key],
        };
        let account_keys = AccountKeys::new(static_keys, Some(&dynamic_keys));
        assert_eq!(
            validate_account_locks(account_keys, MAX_TX_ACCOUNT_LOCKS),
            Err(TransactionError::AccountLoadedTwice)
        );
    }

    #[test]
    fn test_has_duplicates_small() {
        let mut keys = (0..16).map(|_| Pubkey::new_unique()).collect::<Vec<_>>();
        let account_keys = AccountKeys::new(&keys, None);
        assert!(!has_duplicates(account_keys));

        keys[14] = keys[3]; // Duplicate key
        let account_keys = AccountKeys::new(&keys, None);
        assert!(has_duplicates(account_keys));
    }

    #[test]
    fn test_has_duplicates_large() {
        let mut keys = (0..64).map(|_| Pubkey::new_unique()).collect::<Vec<_>>();
        let account_keys = AccountKeys::new(&keys, None);
        assert!(!has_duplicates(account_keys));

        keys[47] = keys[3]; // Duplicate key
        let account_keys = AccountKeys::new(&keys, None);
        assert!(has_duplicates(account_keys));
    }
}
