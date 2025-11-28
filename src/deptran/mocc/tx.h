#pragma once

/**
 * MOCC Transaction
 * 
 * This class extends the OCC transaction with MOCC-specific behavior:
 * - Temperature-aware access (optimistic for cold, pessimistic for hot)
 * - Lock tracking for abort communication
 * - Pre-lock acquisition on retry for hot records
 */

#include "../__dep__.h"
#include "../occ/tx.h"
#include "temperature.h"
#include "mocc_lock.h"

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
 * TxMocc extends TxOcc with MOCC-specific functionality
 */
class TxMocc : public TxOcc {
public:
  using TxOcc::TxOcc;
  
  virtual ~TxMocc();
  
  // ============================================================================
  // Overridden methods for MOCC behavior
  // ============================================================================
  
  /**
   * Read a column with MOCC temperature tracking
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
   * Write a column with MOCC temperature tracking
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
   * Get the list of locks that were acquired during this transaction
   * Used for communicating lock requirements on abort
   */
  std::vector<LockInfo> GetAcquiredLocks() const;
  
  /**
   * Get the list of records that caused conflicts (for abort communication)
   */
  std::vector<LockInfo> GetConflictingRecords() const;
  
  /**
   * Pre-acquire locks for hot records before retry
   * Called by coordinator when retrying a transaction
   * @param locks Locks to pre-acquire
   * @return true if all locks acquired
   */
  bool PreAcquireLocks(const std::vector<LockInfo>& locks);
  
  /**
   * Mark that this is a retry with pre-acquired locks
   */
  void SetRetryWithLocks(bool retry) { is_retry_with_locks_ = retry; }
  bool IsRetryWithLocks() const { return is_retry_with_locks_; }
  
  /**
   * Release all MOCC locks held by this transaction
   */
  void ReleaseMoccLocks();
  
  /**
   * Record that an abort occurred on certain records
   * Updates temperature tracking
   */
  void RecordAbortOnRecords();
  
  /**
   * Record successful commit
   * Updates temperature tracking
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

protected:
  // Track all accesses during this transaction
  std::vector<AccessRecord> access_history_;
  
  // Track which records caused conflicts during validation
  std::vector<std::pair<void*, int>> conflict_records_;
  
  // Flag for retry mode
  bool is_retry_with_locks_{false};
  
  // Pre-acquired locks for retry
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
   * Check if we already have a lock for this record
   */
  bool HasLockFor(void* row, int col_id) const;
};

} // namespace janus




