#pragma once

/**
 * MOCC Coordinator
 * 
 * The MOCC coordinator extends the classic coordinator with:
 * - Hot transaction detection
 * - Lock communication on abort
 * - Pre-acquisition of locks before retry
 * - Broadcasting lock requirements to all shards
 */

#include "../classic/coordinator.h"
#include "mocc_lock.h"
#include "temperature.h"

namespace janus {

/**
 * MoccRetryInfo contains information for retrying a transaction
 */
struct MoccRetryInfo {
  std::vector<LockInfo> required_locks;  // Locks to pre-acquire
  std::map<parid_t, std::vector<LockInfo>> locks_by_shard;  // Locks per shard
  bool is_hot_transaction;
  int retry_count;
  
  MoccRetryInfo() : is_hot_transaction(false), retry_count(0) {}
};

/**
 * CoordinatorMocc extends CoordinatorClassic with MOCC-specific functionality
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
   * Restart a transaction with MOCC lock pre-acquisition
   */
  void Restart() override;
  
  /**
   * Dispatch with MOCC awareness
   */
  virtual void DispatchAsync() override;
  
  /**
   * Handle dispatch acknowledgment with lock info
   */
  void DispatchAckMocc(phase_t phase, int res, TxnOutput& outputs,
                       const std::vector<LockInfo>& held_locks);
  
  /**
   * Prepare phase with MOCC lock collection
   */
  void PrepareMocc();
  
  /**
   * Handle prepare acknowledgment with lock requirements
   */
  void PrepareAckMocc(phase_t phase, int res, 
                      const std::vector<LockInfo>& required_locks);
  
  // ============================================================================
  // MOCC-specific methods
  // ============================================================================
  
  /**
   * Check if current transaction is considered "hot"
   */
  bool IsHotTransaction() const;
  
  /**
   * Collect all locks needed from failed prepare responses
   */
  void CollectRequiredLocks(const std::vector<LockInfo>& locks, parid_t shard);
  
  /**
   * Broadcast lock requirements to all affected shards
   * Called before retry to pre-acquire locks
   */
  bool BroadcastLockRequirements();
  
  /**
   * Pre-acquire locks on all shards before dispatch
   */
  bool PreAcquireLocksOnShards();
  
  /**
   * Get the retry information for the current transaction
   */
  const MoccRetryInfo& GetRetryInfo() const { return retry_info_; }
  
  /**
   * Mark that we received abort with lock info
   */
  void SetAbortWithLockInfo(bool has_lock_info);
  
  /**
   * Get temperature statistics from coordinator perspective
   */
  std::string GetTemperatureReport() const;

protected:
  // MOCC retry information
  MoccRetryInfo retry_info_;
  
  // Track whether abort had lock information
  bool abort_has_lock_info_{false};
  
  // Track locks held per shard
  std::map<parid_t, std::vector<LockInfo>> shard_held_locks_;
  
  // Count of hot record accesses
  int hot_access_count_{0};
  
  /**
   * Aggregate lock requirements from multiple shards
   */
  void AggregateLockRequirements();
  
  /**
   * Determine if we should use pessimistic locking for retry
   */
  bool ShouldUseLocksForRetry() const;
};

} // namespace janus




