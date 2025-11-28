#pragma once

/**
 * MOCC Scheduler
 * 
 * The MOCC scheduler extends the OCC scheduler with:
 * - Temperature-aware validation
 * - Lock communication on abort
 * - Support for pre-locked retry transactions
 */

#include "../occ/scheduler.h"
#include "temperature.h"
#include "mocc_lock.h"

namespace janus {

/**
 * PrepareResult extends the basic success/fail with lock information
 * for distributed abort handling
 */
struct MoccPrepareResult {
  bool success;
  std::vector<LockInfo> required_locks;  // Locks needed for retry
  std::vector<LockInfo> held_locks;      // Locks currently held
  
  MoccPrepareResult() : success(false) {}
  explicit MoccPrepareResult(bool s) : success(s) {}
};

/**
 * SchedulerMocc extends SchedulerOcc with MOCC-specific functionality
 */
class SchedulerMocc : public SchedulerOcc {
public:
  SchedulerMocc();
  virtual ~SchedulerMocc() = default;
  
  /**
   * Override to use MOCC transaction manager
   */
  virtual mdb::Txn* get_mdb_txn(const i64 tid) override;
  
  /**
   * Override DoPrepare for MOCC validation
   * 
   * MOCC validation differs from OCC in that:
   * 1. It tracks which records caused conflicts
   * 2. On failure, it returns the locks needed for retry
   * 3. It updates temperature based on abort/commit
   */
  virtual bool DoPrepare(txnid_t tx_id) override;
  
  /**
   * MOCC-specific prepare that returns lock information
   */
  MoccPrepareResult DoPrepareWithLockInfo(txnid_t tx_id);
  
  /**
   * Override DoCommit to update temperature on success
   */
  virtual void DoCommit(Tx& tx) override;
  
  /**
   * Override DoAbort to update temperature on failure
   */
  void DoAbort(Tx& tx);
  
  /**
   * Pre-acquire locks for a transaction (for retry with locks)
   * Called before dispatching pieces for a retry transaction
   * 
   * @param tx_id Transaction ID
   * @param locks Locks to acquire
   * @return true if all locks acquired successfully
   */
  bool PreAcquireLocksForTransaction(txnid_t tx_id, 
                                     const std::vector<LockInfo>& locks);
  
  /**
   * Get the locks held by a transaction
   */
  std::vector<LockInfo> GetHeldLocks(txnid_t tx_id) const;
  
  /**
   * Release all locks for a transaction
   */
  void ReleaseLocks(txnid_t tx_id);
  
  /**
   * Check if a transaction is considered "hot"
   */
  bool IsHotTransaction(txnid_t tx_id) const;
  
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

protected:
  /**
   * Collect conflict information during validation failure
   */
  std::vector<LockInfo> CollectConflictInfo(txnid_t tx_id);
  
  /**
   * Determine required locks based on the transaction's access pattern
   * and current temperature
   */
  std::vector<LockInfo> DetermineRequiredLocks(txnid_t tx_id);
};

} // namespace janus




