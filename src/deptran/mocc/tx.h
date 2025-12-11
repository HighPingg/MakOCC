#pragma once

/**
 * MOCC Transaction
 * 
 * This class extends the OCC transaction with MOCC-specific behavior:
 * - Temperature-aware access (optimistic for cold, pessimistic for hot)
 * - Canonical mode with MANDATORY restoration (per user requirement)
 * - Wait-Die deadlock prevention via logical timestamps
 * - Lock tracking for proper cleanup on abort/commit
 * 
 * Key MOCC Features (per paper Section 3.3):
 * - Canonical Mode: Locks acquired in globally consistent order
 * - ALWAYS restore canonical mode (user requirement: never skip)
 * - No RLL (Retrospective Lock List) since client doesn't retry
 */

#include "../__dep__.h"
#include "../occ/tx.h"
#include "temperature.h"
#include "mocc_lock.h"
#include "logical_clock.h"

namespace janus {

/**
 * AccessRecord tracks a single read or write access during transaction
 */
struct AccessRecord {
  void* row;
  int col_id;
  bool is_write;
  bool was_hot;           // Was this record hot at access time?
  bool lock_acquired;     // Did we acquire a lock?
  
  AccessRecord(void* r, int c, bool w, bool hot, bool locked)
    : row(r), col_id(c), is_write(w), was_hot(hot), lock_acquired(locked) {}
};

/**
 * CurrentLockEntry represents a lock in the Current Lock List (CLL)
 * Used for canonical mode enforcement
 */
struct CurrentLockEntry {
  void* row;
  int col_id;
  LockMode mode;
  
  CurrentLockEntry(void* r, int c, LockMode m)
    : row(r), col_id(c), mode(m) {}
  
  // Canonical ordering (by row pointer, then column)
  bool operator<(const CurrentLockEntry& other) const {
    if (row != other.row) return row < other.row;
    return col_id < other.col_id;
  }
  
  bool operator==(const CurrentLockEntry& other) const {
    return row == other.row && col_id == other.col_id;
  }
};

/**
 * TxMocc extends TxOcc with MOCC-specific functionality
 * 
 * Key implementation points:
 * - Transaction timestamp assigned at creation for wait-die
 * - Current Lock List (CLL) maintained in sorted order
 * - ALWAYS restore canonical mode on violation (mandatory restoration)
 * - No retry logic (aborts are final per user requirement)
 */
class TxMocc : public TxOcc {
public:
  using TxOcc::TxOcc;
  
  virtual ~TxMocc();
  
  /**
   * Initialize the transaction with a logical timestamp
   * Must be called before any operations
   */
  void InitializeTimestamp(uint32_t coordinator_id);
  
  /**
   * Get transaction's logical timestamp
   */
  const LogicalTimestamp& GetTimestamp() const { return timestamp_; }
  
  // ============================================================================
  // Overridden methods for MOCC behavior
  // ============================================================================
  
  /**
   * Read a column with MOCC temperature tracking and canonical mode
   * For hot records, acquires shared lock pessimistically
   */
  virtual bool ReadColumn(mdb::Row *row,
                          mdb::colid_t col_id,
                          Value *value,
                          int hint_flag = TXN_SAFE) override;

  virtual bool ReadColumns(Row *row,
                           const std::vector<colid_t> &col_ids,
                           std::vector<Value> *values,
                           int hint_flag = TXN_SAFE) override;

  /**
   * Write a column with MOCC temperature tracking and canonical mode
   * For hot records, acquires exclusive lock pessimistically
   */
  virtual bool WriteColumn(Row *row,
                           colid_t col_id,
                           const Value &value,
                           int hint_flag = TXN_SAFE) override;

  virtual bool WriteColumns(Row *row,
                            const std::vector<colid_t> &col_ids,
                            const std::vector<Value> &values,
                            int hint_flag = TXN_SAFE) override;
  
  // ============================================================================
  // MOCC-specific methods
  // ============================================================================
  
  /**
   * Check if this transaction should use pessimistic locking for a record
   */
  bool ShouldLockPessimistically(void* row, int col_id) const;
  
  /**
   * Acquire a lock with canonical mode enforcement
   * ALWAYS restores canonical mode on violation (mandatory per user requirement)
   * 
   * @param row Row pointer
   * @param col_id Column ID
   * @param mode Lock mode
   * @return true if lock acquired, false if must abort
   */
  bool AcquireLockCanonical(void* row, int col_id, LockMode mode);
  
  /**
   * Get the list of locks that were acquired during this transaction
   */
  std::vector<LockInfo> GetAcquiredLocks() const;
  
  /**
   * Get the list of records that caused conflicts (for temperature update)
   */
  std::vector<LockInfo> GetConflictingRecords() const;
  
  /**
   * Pre-acquire locks for hot records (sorted order for deadlock prevention)
   * @param locks Locks to pre-acquire
   * @return true if all locks acquired
   */
  bool PreAcquireLocks(const std::vector<LockInfo>& locks);
  
  /**
   * Mark that this is a hot transaction (for coordinator tracking)
   */
  void SetRetryWithLocks(bool retry) { is_retry_with_locks_ = retry; }
  bool IsRetryWithLocks() const { return is_retry_with_locks_; }
  
  /**
   * Release all MOCC locks held by this transaction
   * MUST be called on both commit and abort
   */
  void ReleaseMoccLocks();
  
  /**
   * Record that an abort occurred on certain records
   * Updates temperature tracking
   */
  void RecordAbortOnRecords();
  
  /**
   * Record successful commit
   * Updates temperature tracking (no-op in MOCC, kept for API)
   */
  void RecordCommitOnRecords();
  
  /**
   * Check if this transaction is considered "hot" (touches many hot records)
   */
  bool IsHotTransaction() const;
  
  /**
   * Get access history for this transaction
   */
  const std::vector<AccessRecord>& GetAccessHistory() const { return access_history_; }
  
  /**
   * Add conflict record (called during validation failure)
   */
  void AddConflictRecord(void* row, int col_id);

protected:
  // Transaction's logical timestamp for wait-die
  LogicalTimestamp timestamp_;
  
  // Track all accesses during this transaction
  std::vector<AccessRecord> access_history_;
  
  // Current Lock List (CLL) - maintained in sorted order for canonical mode
  std::vector<CurrentLockEntry> current_lock_list_;
  
  // Track which records caused conflicts during validation
  std::vector<std::pair<void*, int>> conflict_records_;
  
  // Flag for hot transaction mode
  bool is_retry_with_locks_{false};
  
  // Pre-acquired locks (empty since no retry)
  std::vector<LockInfo> pre_acquired_locks_;
  
  // Count of hot record accesses
  int hot_access_count_{0};
  
  // Threshold for considering transaction as "hot"
  static constexpr int HOT_TXN_THRESHOLD = 3;
  
  /**
   * Internal method to record an access
   */
  void RecordAccess(void* row, int col_id, bool is_write);
  
  /**
   * Check if we already have a lock for this record in CLL
   */
  bool HasLockFor(void* row, int col_id) const;
  
  /**
   * Get lock mode if we already have this lock
   */
  LockMode GetHeldLockMode(void* row, int col_id) const;
  
  /**
   * Check for canonical mode violations and restore if needed
   * Returns locks that need to be released to restore canonical mode
   */
  std::vector<CurrentLockEntry> GetCanonicalViolations(void* new_row, int new_col_id);
  
  /**
   * Restore canonical mode by releasing violating locks
   * ALWAYS succeeds (mandatory restoration per user requirement)
   */
  void RestoreCanonicalMode(const std::vector<CurrentLockEntry>& violations);
  
  /**
   * Add lock to CLL in sorted order
   */
  void AddToCLL(void* row, int col_id, LockMode mode);
  
  /**
   * Remove lock from CLL
   */
  void RemoveFromCLL(void* row, int col_id);
};

} // namespace janus
