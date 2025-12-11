#pragma once

/**
 * MOCC Locking Data Structure with Wait-Die Deadlock Prevention
 * 
 * This module implements the hybrid locking mechanism for MOCC with:
 * - Wait-Die protocol for distributed deadlock prevention
 * - Reader-writer locks (multiple readers OR single writer)
 * - Try-lock semantics for non-blocking attempts
 * - Integration with temperature tracking
 * - Canonical mode support (lock ordering by timestamp)
 * 
 * Wait-Die Protocol:
 * - Older transaction (lower timestamp) WAITS for younger
 * - Younger transaction (higher timestamp) DIES (aborts) if blocked by older
 * 
 * This guarantees no deadlocks without expensive distributed detection.
 */

#include "../__dep__.h"
#include "temperature.h"
#include "logical_clock.h"
#include <atomic>
#include <unordered_map>
#include <unordered_set>
#include <shared_mutex>
#include <queue>
#include <condition_variable>
#include <list>

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
  WAITING = 1,      // Lock request queued, waiting (older txn waiting)
  DENIED = 2,       // Lock denied - would deadlock (younger txn must abort)
  TIMEOUT = 3       // Lock request timed out
};

/**
 * LockHolder tracks a transaction holding a lock
 */
struct LockHolder {
  txnid_t txn_id;
  LogicalTimestamp timestamp;
  LockMode mode;
  
  LockHolder(txnid_t tid, LogicalTimestamp ts, LockMode m)
    : txn_id(tid), timestamp(ts), mode(m) {}
};

/**
 * LockWaiter tracks a transaction waiting for a lock
 */
struct LockWaiter {
  txnid_t txn_id;
  LogicalTimestamp timestamp;
  LockMode mode;
  std::shared_ptr<std::condition_variable> cv;
  std::atomic<bool> granted{false};
  std::atomic<bool> should_abort{false};
  
  LockWaiter(txnid_t tid, LogicalTimestamp ts, LockMode m)
    : txn_id(tid), timestamp(ts), mode(m), cv(std::make_shared<std::condition_variable>()) {}
};

/**
 * RecordLock manages the lock state for a single record
 * Implements reader-writer lock with Wait-Die deadlock prevention
 */
class RecordLock {
public:
  RecordLock();
  
  /**
   * Attempt to acquire the lock using Wait-Die protocol
   * 
   * @param txn_id Transaction requesting the lock
   * @param timestamp Logical timestamp of the transaction
   * @param mode Shared (read) or Exclusive (write)
   * @param blocking Whether to block if we should wait
   * @return LockStatus:
   *         - ACQUIRED: Lock granted
   *         - WAITING: Older txn waiting for younger (blocks if blocking=true)
   *         - DENIED: Younger txn must abort (wait-die: die)
   */
  LockStatus Acquire(txnid_t txn_id, LogicalTimestamp timestamp, 
                     LockMode mode, bool blocking = false);
  
  /**
   * Try to acquire lock without blocking (for canonical mode)
   * @return ACQUIRED if successful, DENIED otherwise
   */
  LockStatus TryAcquire(txnid_t txn_id, LogicalTimestamp timestamp, LockMode mode);
  
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
   * Get the holder with the minimum timestamp (oldest holder)
   */
  LogicalTimestamp GetOldestHolderTimestamp() const;
  
  /**
   * Get all current holders
   */
  std::vector<LockHolder> GetHolders() const;
  
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
  
  // Current lock holders
  std::vector<LockHolder> holders_;
  
  // Wait list for waiters (ordered by timestamp for fairness)
  std::list<std::shared_ptr<LockWaiter>> waiters_;
  
  /**
   * Check if a mode can be granted given current holders
   */
  bool CanGrant(LockMode mode, txnid_t txn_id) const;
  
  /**
   * Apply wait-die protocol
   * @return true if requester should wait, false if should abort
   */
  bool ShouldWait(const LogicalTimestamp& requester_ts) const;
  
  /**
   * Grant lock to waiters that can now proceed
   */
  void GrantWaiters();
  
  /**
   * Find oldest holder timestamp
   */
  LogicalTimestamp FindOldestHolder() const;
};

/**
 * LockInfo contains information about locks needed by a transaction
 * Used for canonical mode and lock communication
 */
struct LockInfo {
  void* row;
  int col_id;
  LockMode mode;
  LogicalTimestamp timestamp;  // Timestamp of the record (for ordering)
  
  LockInfo(void* r, int c, LockMode m)
    : row(r), col_id(c), mode(m), timestamp() {}
    
  LockInfo(void* r, int c, LockMode m, LogicalTimestamp ts)
    : row(r), col_id(c), mode(m), timestamp(ts) {}
    
  /**
   * Compare locks for canonical ordering
   * Uses row pointer and column for consistent ordering
   */
  bool operator<(const LockInfo& other) const {
    if (row != other.row) return row < other.row;
    return col_id < other.col_id;
  }
  
  bool operator==(const LockInfo& other) const {
    return row == other.row && col_id == other.col_id;
  }
};

/**
 * RowColKey for lock map
 */
struct RowColKey {
  void* row;
  int col_id;
  
  bool operator==(const RowColKey& other) const {
    return row == other.row && col_id == other.col_id;
  }
};

struct RowColKeyHash {
  size_t operator()(const RowColKey& key) const {
    return std::hash<void*>()(key.row) ^ 
           (std::hash<int>()(key.col_id) << 1);
  }
};

/**
 * TransactionLockState tracks lock state for a single transaction
 * Used for canonical mode enforcement
 */
struct TransactionLockState {
  txnid_t txn_id;
  LogicalTimestamp timestamp;
  std::vector<LockInfo> held_locks;  // Sorted by (row, col_id)
  
  TransactionLockState(txnid_t tid, LogicalTimestamp ts)
    : txn_id(tid), timestamp(ts) {}
};

/**
 * MoccLockManager is a singleton that manages all locks in the system
 * with Wait-Die deadlock prevention and canonical mode support
 */
class MoccLockManager {
public:
  static MoccLockManager& Instance() {
    static MoccLockManager instance;
    return instance;
  }
  
  /**
   * Register a transaction with its timestamp
   * Must be called before acquiring any locks
   */
  void RegisterTransaction(txnid_t txn_id, LogicalTimestamp timestamp);
  
  /**
   * Unregister a transaction (on commit or abort)
   */
  void UnregisterTransaction(txnid_t txn_id);
  
  /**
   * Acquire a lock on a record using Wait-Die
   * 
   * @param txn_id Transaction ID
   * @param row Row pointer
   * @param col_id Column ID (-1 for entire row)
   * @param mode Lock mode (SHARED or EXCLUSIVE)
   * @param blocking Whether to block on wait (for canonical mode: true, otherwise: false)
   * @return LockStatus indicating result
   */
  LockStatus AcquireLock(txnid_t txn_id, void* row, int col_id, 
                         LockMode mode, bool blocking = false);
  
  /**
   * Try to acquire lock without blocking (canonical mode alternative)
   */
  LockStatus TryAcquireLock(txnid_t txn_id, void* row, int col_id, LockMode mode);
  
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
   * Get all locks held by a transaction (sorted for canonical mode)
   */
  std::vector<LockInfo> GetHeldLocks(txnid_t txn_id) const;
  
  /**
   * Get transaction's timestamp
   */
  LogicalTimestamp GetTransactionTimestamp(txnid_t txn_id) const;
  
  /**
   * Pre-acquire locks for a set of records (sorted order for deadlock prevention)
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
    size_t registered_txns;
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
  
  // Transaction state tracking
  mutable std::mutex txn_mutex_;
  std::unordered_map<txnid_t, TransactionLockState> txn_states_;
};

} // namespace janus
