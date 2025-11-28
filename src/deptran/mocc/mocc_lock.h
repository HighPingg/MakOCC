#pragma once

/**
 * MOCC Locking Data Structure
 * 
 * This module implements the hybrid locking mechanism for MOCC.
 * It provides both read and write locks that can be acquired 
 * pessimistically (for hot records) or optimistically validated
 * (for cold records).
 * 
 * The lock structure supports:
 * - Multiple concurrent readers OR single writer
 * - Try-lock semantics for non-blocking attempts
 * - Lock queue for fairness
 * - Integration with temperature tracking
 */

#include "../__dep__.h"
#include "temperature.h"
#include <atomic>
#include <unordered_map>
#include <unordered_set>
#include <shared_mutex>
#include <queue>
#include <condition_variable>

namespace janus {

// Forward declarations
class MoccLockManager;

/**
 * LockMode represents the type of lock being requested
 */
enum class LockMode {
  NONE = 0,
  SHARED = 1,     // Read lock - multiple concurrent holders allowed
  EXCLUSIVE = 2   // Write lock - single holder only
};

/**
 * LockStatus represents the result of a lock operation
 */
enum class LockStatus {
  ACQUIRED = 0,     // Lock successfully acquired
  WAITING = 1,      // Lock request queued, waiting
  DENIED = 2,       // Lock denied (conflict, would deadlock)
  TIMEOUT = 3       // Lock request timed out
};

/**
 * LockRequest represents a pending or held lock
 */
struct LockRequest {
  txnid_t txn_id;
  LockMode mode;
  bool granted;
  
  LockRequest(txnid_t tid, LockMode m)
    : txn_id(tid), mode(m), granted(false) {}
};

/**
 * RecordLock manages the lock state for a single record
 * Implements a reader-writer lock with queue-based fairness
 */
class RecordLock {
public:
  RecordLock();
  
  /**
   * Attempt to acquire the lock
   * @param txn_id Transaction requesting the lock
   * @param mode Shared (read) or Exclusive (write)
   * @param blocking Whether to block until acquired
   * @return LockStatus indicating result
   */
  LockStatus Acquire(txnid_t txn_id, LockMode mode, bool blocking = false);
  
  /**
   * Try to acquire lock without blocking
   * @return true if acquired, false if would block
   */
  bool TryAcquire(txnid_t txn_id, LockMode mode);
  
  /**
   * Release the lock held by a transaction
   * @param txn_id Transaction releasing the lock
   * @return true if lock was released, false if not held
   */
  bool Release(txnid_t txn_id);
  
  /**
   * Check if a transaction holds the lock
   */
  bool IsHeldBy(txnid_t txn_id) const;
  
  /**
   * Check if the lock is held in exclusive mode
   */
  bool IsExclusivelyHeld() const;
  
  /**
   * Get the holder of an exclusive lock (0 if not exclusively held)
   */
  txnid_t GetExclusiveHolder() const;
  
  /**
   * Get all transactions holding shared locks
   */
  std::vector<txnid_t> GetSharedHolders() const;
  
  /**
   * Get number of waiters
   */
  size_t GetWaiterCount() const;
  
  /**
   * Check if lock is free
   */
  bool IsFree() const;

private:
  mutable std::mutex mutex_;
  std::condition_variable cv_;
  
  // Current lock holders
  txnid_t exclusive_holder_{0};  // 0 means no exclusive holder
  std::unordered_set<txnid_t> shared_holders_;
  
  // Wait queue for fairness
  std::queue<LockRequest> wait_queue_;
  
  // Grant queued requests if possible
  void GrantWaiters();
  
  // Check if a mode can be granted given current state
  bool CanGrant(LockMode mode, txnid_t txn_id) const;
};

/**
 * LockInfo contains information about locks needed by a transaction
 * Used for communicating lock requirements on abort
 */
struct LockInfo {
  void* row;
  int col_id;
  LockMode mode;
  
  LockInfo(void* r, int c, LockMode m)
    : row(r), col_id(c), mode(m) {}
};

/**
 * MoccLockManager is a singleton that manages all locks in the system
 * It integrates with TemperatureTracker to decide when to use locks
 */
class MoccLockManager {
public:
  static MoccLockManager& Instance() {
    static MoccLockManager instance;
    return instance;
  }
  
  /**
   * Acquire a lock on a record
   * @param txn_id Transaction ID
   * @param row Row pointer
   * @param col_id Column ID (-1 for entire row)
   * @param mode Lock mode (SHARED or EXCLUSIVE)
   * @param force Force pessimistic lock regardless of temperature
   * @return LockStatus indicating result
   */
  LockStatus AcquireLock(txnid_t txn_id, void* row, int col_id, 
                         LockMode mode, bool force = false);
  
  /**
   * Try to acquire lock without blocking
   */
  bool TryAcquireLock(txnid_t txn_id, void* row, int col_id, LockMode mode);
  
  /**
   * Release a specific lock
   */
  bool ReleaseLock(txnid_t txn_id, void* row, int col_id);
  
  /**
   * Release all locks held by a transaction
   */
  void ReleaseAllLocks(txnid_t txn_id);
  
  /**
   * Check if a transaction holds a lock
   */
  bool HoldsLock(txnid_t txn_id, void* row, int col_id) const;
  
  /**
   * Check if a record should use pessimistic locking based on temperature
   */
  bool ShouldUsePessimisticLock(void* row, int col_id) const;
  
  /**
   * Get all locks held by a transaction
   */
  std::vector<LockInfo> GetHeldLocks(txnid_t txn_id) const;
  
  /**
   * Get locks that would be needed to execute operations on given records
   * Used for pre-acquiring locks on retry
   */
  std::vector<LockInfo> GetRequiredLocks(const std::vector<std::pair<void*, int>>& records,
                                         bool write) const;
  
  /**
   * Pre-acquire locks for a set of records (for hot transaction retry)
   * @param txn_id Transaction ID
   * @param locks Locks to acquire
   * @return true if all locks acquired, false otherwise
   */
  bool PreAcquireLocks(txnid_t txn_id, const std::vector<LockInfo>& locks);
  
  /**
   * Clear all locks (for testing/reset)
   */
  void Clear();
  
  /**
   * Get statistics
   */
  struct Stats {
    size_t total_locks;
    size_t held_locks;
    size_t waiting_requests;
  };
  Stats GetStats() const;

private:
  MoccLockManager() = default;
  ~MoccLockManager() = default;
  
  MoccLockManager(const MoccLockManager&) = delete;
  MoccLockManager& operator=(const MoccLockManager&) = delete;
  
  // Get or create lock for a record
  RecordLock& GetOrCreateLock(void* row, int col_id);
  
  // Lock storage
  mutable std::shared_mutex mutex_;
  std::unordered_map<RowColKey, std::unique_ptr<RecordLock>, RowColKeyHash> locks_;
  
  // Track which locks each transaction holds (for ReleaseAllLocks)
  mutable std::mutex txn_locks_mutex_;
  std::unordered_map<txnid_t, std::vector<RowColKey>> txn_held_locks_;
};

} // namespace janus




