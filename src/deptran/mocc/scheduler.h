#pragma once

/**
 * MOCC Scheduler
 * 
 * The MOCC scheduler extends the OCC scheduler with:
 * - Temperature-aware DoPrepare: hot vs cold record handling
 * - SHARED locks for hot READS (prevent clobbered reads)
 * - EXCLUSIVE locks for hot WRITES
 * - OCC validation for cold records only
 * - ALWAYS release all locks on commit/abort
 */

#include "../occ/scheduler.h"
#include "temperature.h"
#include "mocc_lock.h"
#include "logical_clock.h"

namespace janus {

/**
 * PrepareResult extends the basic success/fail with lock information
 */
struct MoccPrepareResult {
  bool success;
  std::vector<LockInfo> conflicting_records;  // Records that caused conflicts
  
  MoccPrepareResult() : success(false) {}
  explicit MoccPrepareResult(bool s) : success(s) {}
};

/**
 * SchedulerMocc extends SchedulerOcc with MOCC-specific functionality
 * 
 * Key implementation (MOCC paper Algorithm 1, commit() function):
 * 
 * DoPrepare for MOCC:
 * 1. Acquire SHARED locks for hot records in read set
 * 2. Acquire EXCLUSIVE locks for hot records in write set
 * 3. OCC validation (version check) for cold records
 * 4. OCC lock acquisition for cold records
 * 
 * DoCommit:
 * - Apply writes
 * - Release ALL locks (both MOCC and OCC)
 * 
 * HandleAbort:
 * - Update temperature for ALL accessed records
 * - Release ALL locks (both MOCC and OCC)
 */
class SchedulerMocc : public SchedulerOcc {
public:
  SchedulerMocc();
  virtual ~SchedulerMocc() = default;
  
  /**
   * Override to use MOCC transaction
   */
  virtual mdb::Txn* get_mdb_txn(const i64 tid) override;
  
  /**
   * Override DoPrepare for MOCC validation
   * 
   * MOCC validation:
   * 1. For hot records: already have locks from execution phase
   * 2. For cold records: OCC version check + lock acquisition
   * 3. On failure: update temperature and release all locks
   */
  virtual bool DoPrepare(txnid_t tx_id) override;
  
  /**
   * MOCC-specific prepare with detailed result
   */
  MoccPrepareResult DoPrepareWithResult(txnid_t tx_id);
  
  /**
   * Override DoCommit to release all MOCC locks
   * MUST release all locks!
   */
  virtual void DoCommit(Tx& tx) override;
  
  /**
   * DoAbort: update temperature and release all locks
   * MUST release all locks!
   */
  void DoAbort(Tx& tx);
  
  /**
   * Handle abort with temperature update
   */
  void HandleAbort(txnid_t tx_id);
  
  /**
   * Release all locks for a transaction
   */
  void ReleaseLocks(txnid_t tx_id);
  
  /**
   * Check if a record is hot
   */
  bool IsHot(void* row, int col_id = -1) const;
  
  /**
   * Apply temperature decay (should be called periodically)
   */
  void ApplyTemperatureDecay();
  
  /**
   * Get temperature statistics for monitoring
   */
  TemperatureTracker::Stats GetTemperatureStats() const;
  
  /**
   * Get lock statistics for monitoring
   */
  MoccLockManager::Stats GetLockStats() const;
  
  /**
   * Initialize transaction with timestamp
   */
  void InitializeTransaction(txnid_t tx_id, LogicalTimestamp timestamp);

protected:
  /**
   * Acquire locks for hot records in read set
   * @return true if all locks acquired, false if should abort
   */
  bool AcquireReadLocksForHotRecords(txnid_t tx_id, mdb::TxnOCC* txn);
  
  /**
   * Acquire locks for hot records in write set
   * @return true if all locks acquired, false if should abort
   */
  bool AcquireWriteLocksForHotRecords(txnid_t tx_id, mdb::TxnOCC* txn);
  
  /**
   * Perform OCC validation for cold records
   * @return true if validation passed, false if should abort
   */
  bool ValidateColdRecords(txnid_t tx_id, mdb::TxnOCC* txn);
  
  /**
   * Release all OCC locks (row locks in TxnOCC)
   */
  void ReleaseOccLocks(mdb::TxnOCC* txn);
  
  /**
   * Update temperature on abort for all accessed records
   */
  void UpdateTemperatureOnAbort(txnid_t tx_id);
};

} // namespace janus
