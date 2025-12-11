#pragma once

/**
 * MOCC Coordinator
 * 
 * The MOCC coordinator extends the classic 2PC coordinator with:
 * - Logical timestamp assignment for wait-die
 * - NO RETRY (user requirement: aborts are final)
 * - Temperature-aware transaction execution
 * - Lock cleanup on abort
 * 
 * Key differences from MOCC paper:
 * - No RLL (Retrospective Lock List) since we don't retry
 * - Abort is always final - just clean up and report failure
 */

#include "../classic/coordinator.h"
#include "mocc_lock.h"
#include "temperature.h"
#include "logical_clock.h"

namespace janus {

/**
 * CoordinatorMocc extends CoordinatorClassic with MOCC-specific functionality
 * 
 * Transaction Flow:
 * 1. GotTxData: Assign logical timestamp
 * 2. DispatchAsync: Send with timestamp
 * 3. DoPrepare: Hot/cold validation
 * 4. DoCommit/DoAbort: Clean up locks
 */
class CoordinatorMocc : public CoordinatorClassic {
public:
  CoordinatorMocc(uint32_t coo_id,
                  int benchmark,
                  ClientControlServiceImpl* ccsi,
                  uint32_t thread_id);
  
  virtual ~CoordinatorMocc() = default;
  
  // ============================================================================
  // Override transaction lifecycle methods
  // ============================================================================
  
  /**
   * Reset coordinator state (including MOCC-specific state)
   */
  virtual void Reset() override;
  
  /**
   * Called when transaction data is received
   * Assigns logical timestamp here
   */
  virtual void GotTxData();
  
  /**
   * Restart - DISABLED for MOCC (no retries per user requirement)
   * Always aborts instead of retrying
   */
  void Restart() override;
  
  /**
   * Dispatch with MOCC timestamp
   */
  virtual void DispatchAsync() override;
  
  /**
   * Commit - must release all locks
   */
  virtual void Commit() override;
  
  // ============================================================================
  // MOCC-specific methods
  // ============================================================================
  
  /**
   * Get transaction's logical timestamp
   */
  const LogicalTimestamp& GetTimestamp() const { return txn_timestamp_; }
  
  /**
   * Check if current transaction is considered "hot"
   */
  bool IsHotTransaction() const;
  
  /**
   * Get temperature statistics from coordinator perspective
   */
  std::string GetTemperatureReport() const;
  
  /**
   * Generate MOCC logical timestamp (using coordinator ID)
   */
  LogicalTimestamp GenerateMoccTimestamp();

protected:
  // Transaction's logical timestamp for wait-die
  LogicalTimestamp txn_timestamp_;
  
  // Logical clock for this coordinator
  LogicalClock clock_;
  
  // Track if transaction touched hot records
  bool touched_hot_records_{false};
  
  // Count of hot record accesses
  int hot_access_count_{0};
  
  /**
   * Handle abort without retry (user requirement)
   */
  void HandleFinalAbort();
  
  /**
   * Clean up locks on all shards
   */
  void CleanupLocks();
};

} // namespace janus
