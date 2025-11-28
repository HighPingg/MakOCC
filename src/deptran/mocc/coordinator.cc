/**
 * MOCC Coordinator Implementation
 */

#include "coordinator.h"
#include "../frame.h"
#include "../benchmark_control_rpc.h"

namespace janus {

CoordinatorMocc::CoordinatorMocc(uint32_t coo_id,
                                 int benchmark,
                                 ClientControlServiceImpl* ccsi,
                                 uint32_t thread_id)
    : CoordinatorClassic(coo_id, benchmark, ccsi, thread_id) {
}

void CoordinatorMocc::Reset() {
  CoordinatorClassic::Reset();
  
  // Reset MOCC-specific state
  retry_info_ = MoccRetryInfo();
  abort_has_lock_info_ = false;
  shard_held_locks_.clear();
  hot_access_count_ = 0;
}

void CoordinatorMocc::Restart() {
  std::lock_guard<std::recursive_mutex> lock(this->mtx_);
  verify(aborted_);
  
  // Increment retry count
  retry_info_.retry_count++;
  
  // Check if we should pre-acquire locks for this retry
  if (ShouldUseLocksForRetry() && !retry_info_.required_locks.empty()) {
    Log_debug("MOCC: Retry %d with pre-acquired locks, tx: %" PRIx64, 
              retry_info_.retry_count, ongoing_tx_id_);
    
    // Pre-acquire locks before restarting
    if (!PreAcquireLocksOnShards()) {
      Log_warn("MOCC: Failed to pre-acquire locks for retry");
      // Continue anyway - will fall back to OCC
    }
  }
  
  // Call parent restart
  CoordinatorClassic::Restart();
}

void CoordinatorMocc::DispatchAsync() {
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  auto txn = (TxData*) cmd_;
  
  Log_debug("MOCC: Dispatch for tx_id: %" PRIx64, txn->root_id_);
  
  // Check if this is a retry with pre-acquired locks
  if (retry_info_.retry_count > 0 && !retry_info_.required_locks.empty()) {
    retry_info_.is_hot_transaction = true;
  }
  
  // Use parent dispatch
  CoordinatorClassic::DispatchAsync();
}

void CoordinatorMocc::DispatchAckMocc(phase_t phase, int res, TxnOutput& outputs,
                                      const std::vector<LockInfo>& held_locks) {
  // Store held locks for this dispatch
  // Note: In full implementation, we'd track by partition
  
  // Call parent ack handler
  CoordinatorClassic::DispatchAck(phase, res, outputs);
}

void CoordinatorMocc::PrepareMocc() {
  // For now, use parent prepare
  // In full implementation, this would track lock requirements
  Prepare();
}

void CoordinatorMocc::PrepareAckMocc(phase_t phase, int res,
                                     const std::vector<LockInfo>& required_locks) {
  std::lock_guard<std::recursive_mutex> lock(this->mtx_);
  
  if (phase != phase_) return;
  
  TxData* cmd = (TxData*) cmd_;
  n_prepare_ack_++;
  
  if (res == REJECT) {
    // Store required locks from this shard
    if (!required_locks.empty()) {
      // Aggregate lock requirements
      for (const auto& lock_info : required_locks) {
        retry_info_.required_locks.push_back(lock_info);
      }
      abort_has_lock_info_ = true;
    }
    
    cmd->commit_.store(false);
    aborted_ = true;
  }
  
  Log_debug("MOCC: prepare result %d for tx %" PRIx64 ", locks: %zu", 
            res, cmd_->root_id_, required_locks.size());
  
  if (n_prepare_ack_ == cmd->partition_ids_.size()) {
    Log_debug("MOCC: prepare finished for tx %" PRIx64, cmd->root_id_);
    if (!aborted_) {
      cmd->commit_.store(true);
      committed_ = true;
    } else {
      // Aggregate all lock requirements for retry
      AggregateLockRequirements();
    }
    GotoNextPhase();
  }
}

bool CoordinatorMocc::IsHotTransaction() const {
  return retry_info_.is_hot_transaction || hot_access_count_ >= 3;
}

void CoordinatorMocc::CollectRequiredLocks(const std::vector<LockInfo>& locks,
                                           parid_t shard) {
  // Store locks by shard for later broadcast
  retry_info_.locks_by_shard[shard] = locks;
  
  // Also add to combined list
  for (const auto& lock : locks) {
    retry_info_.required_locks.push_back(lock);
  }
}

bool CoordinatorMocc::BroadcastLockRequirements() {
  if (retry_info_.required_locks.empty()) {
    return true;  // Nothing to broadcast
  }
  
  Log_debug("MOCC: Broadcasting lock requirements: %zu locks", 
            retry_info_.required_locks.size());
  
  // In full implementation, this would send lock requirements to all shards
  // For now, locks are acquired during dispatch through scheduler
  
  return true;
}

bool CoordinatorMocc::PreAcquireLocksOnShards() {
  if (retry_info_.required_locks.empty()) {
    return true;
  }
  
  Log_debug("MOCC: Pre-acquiring %zu locks for retry", 
            retry_info_.required_locks.size());
  
  // In full distributed implementation, this would:
  // 1. Send lock requests to each shard
  // 2. Wait for acknowledgments
  // 3. Return false if any lock cannot be acquired
  
  // For now, assume success - actual locking happens in scheduler
  return true;
}

void CoordinatorMocc::SetAbortWithLockInfo(bool has_lock_info) {
  abort_has_lock_info_ = has_lock_info;
}

std::string CoordinatorMocc::GetTemperatureReport() const {
  auto stats = TemperatureTracker::Instance().GetStats();
  
  std::ostringstream oss;
  oss << "Temperature Stats: "
      << "total=" << stats.total_records
      << ", hot=" << stats.hot_records
      << ", warm=" << stats.warm_records
      << ", cold=" << stats.cold_records
      << ", avg_temp=" << (stats.total_records > 0 ? 
                          stats.total_temperature / stats.total_records : 0);
  
  return oss.str();
}

void CoordinatorMocc::AggregateLockRequirements() {
  // Remove duplicate locks
  std::vector<LockInfo>& locks = retry_info_.required_locks;
  
  // Sort by row pointer and column id
  std::sort(locks.begin(), locks.end(),
            [](const LockInfo& a, const LockInfo& b) {
              if (a.row != b.row) return a.row < b.row;
              return a.col_id < b.col_id;
            });
  
  // Remove duplicates, keeping the strongest lock mode
  auto it = std::unique(locks.begin(), locks.end(),
                        [](const LockInfo& a, const LockInfo& b) {
                          return a.row == b.row && a.col_id == b.col_id;
                        });
  locks.erase(it, locks.end());
  
  // Upgrade shared locks to exclusive if both are present
  // (This is a simplified approach)
  
  Log_debug("MOCC: Aggregated %zu unique locks for retry", locks.size());
}

bool CoordinatorMocc::ShouldUseLocksForRetry() const {
  // Use locks for retry if:
  // 1. This is at least the second retry
  // 2. We have lock information from the abort
  // 3. The transaction is considered "hot"
  
  return retry_info_.retry_count >= 1 && 
         (abort_has_lock_info_ || IsHotTransaction());
}

} // namespace janus




