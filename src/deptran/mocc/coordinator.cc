/**
 * MOCC Coordinator Implementation
 * 
 * Key features:
 * - Assigns logical timestamps for wait-die
 * - NO RETRY (user requirement: aborts are final)
 * - Clean up locks on abort/commit
 */

#include "coordinator.h"
#include "../frame.h"
#include "../benchmark_control_rpc.h"
#include "scheduler.h"

namespace janus {

CoordinatorMocc::CoordinatorMocc(uint32_t coo_id,
                                 int benchmark,
                                 ClientControlServiceImpl* ccsi,
                                 uint32_t thread_id)
    : CoordinatorClassic(coo_id, benchmark, ccsi, thread_id),
      clock_(coo_id) {  // Initialize logical clock with coordinator ID
  
  // Initialize the global clock manager with this coordinator's ID
  LogicalClockManager::Instance().Initialize(coo_id);
}

void CoordinatorMocc::Reset() {
  CoordinatorClassic::Reset();
  
  // Reset MOCC-specific state
  txn_timestamp_ = LogicalTimestamp();
  touched_hot_records_ = false;
  hot_access_count_ = 0;
}

void CoordinatorMocc::GotTxData() {
  // Assign logical timestamp when we receive transaction data
  // This happens BEFORE dispatch
  txn_timestamp_ = GenerateMoccTimestamp();
  
  Log_debug("MOCC: Assigned timestamp (%lu,%u) to tx %" PRIx64,
            txn_timestamp_.clock, txn_timestamp_.node_id, ongoing_tx_id_);
}

LogicalTimestamp CoordinatorMocc::GenerateMoccTimestamp() {
  return clock_.GetTimestamp();
}

void CoordinatorMocc::Restart() {
  // USER REQUIREMENT: NO RETRY
  // Aborts are always final - do not restart
  
  Log_debug("MOCC: Abort is final (no retry) for tx %" PRIx64, ongoing_tx_id_);
  
  // Clean up any held locks
  CleanupLocks();
  
  // Handle final abort - notify client of failure
  HandleFinalAbort();
}

void CoordinatorMocc::DispatchAsync() {
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  
  // Ensure we have a timestamp
  if (!txn_timestamp_.IsValid()) {
    txn_timestamp_ = GenerateMoccTimestamp();
  }
  
  auto txn = (TxData*) cmd_;
  
  Log_debug("MOCC: Dispatch for tx_id: %" PRIx64 " with timestamp (%lu,%u)",
            txn->root_id_, txn_timestamp_.clock, txn_timestamp_.node_id);
  
  // Use parent dispatch
  // NOTE: In full implementation, we would send timestamp with dispatch RPC
  // The timestamp would be included in the RPC message so shards can use wait-die
  CoordinatorClassic::DispatchAsync();
}

void CoordinatorMocc::Commit() {
  Log_debug("MOCC: Commit for tx %" PRIx64, ongoing_tx_id_);
  
  // Use parent commit - this will trigger DoCommit on schedulers
  // which will release all locks
  CoordinatorClassic::Commit();
  
  // Coordinator-side cleanup (if any)
  touched_hot_records_ = false;
  hot_access_count_ = 0;
}

void CoordinatorMocc::HandleFinalAbort() {
  // USER REQUIREMENT: Aborts are final - no retry
  // Just clean up and let the client handle the failure
  
  Log_debug("MOCC: Final abort for tx %" PRIx64 " (no retry)", ongoing_tx_id_);
  
  // Temperature will be updated by scheduler on each shard during abort
  // This helps future transactions avoid the same conflicts
}

void CoordinatorMocc::CleanupLocks() {
  // Clean up any coordinator-side lock tracking
  // Actual lock release happens on schedulers via RPC
  
  Log_debug("MOCC: Cleaning up locks for tx %" PRIx64, ongoing_tx_id_);
  
  // Note: In distributed setting, scheduler on each shard handles lock cleanup
  // via the Abort RPC. The MoccLockManager::ReleaseAllLocks() is called
  // in DoAbort() on each scheduler.
}

bool CoordinatorMocc::IsHotTransaction() const {
  return touched_hot_records_ || hot_access_count_ >= 3;
}

std::string CoordinatorMocc::GetTemperatureReport() const {
  auto stats = TemperatureTracker::Instance().GetStats();
  
  std::ostringstream oss;
  oss << "Temperature Stats: "
      << "total=" << stats.total_records
      << ", hot=" << stats.hot_records
      << ", warm=" << stats.warm_records
      << ", cold=" << stats.cold_records;
  
  if (stats.total_records > 0) {
    oss << ", avg_temp=" << (stats.total_temperature / stats.total_records);
  }
  
  return oss.str();
}

} // namespace janus
