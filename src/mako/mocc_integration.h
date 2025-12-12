/**
 * MOCC Integration Layer for Mako/STO
 * 
 * This header provides hooks to integrate MOCC temperature tracking
 * with Mako's STO-based transaction system.
 * 
 * Key Features:
 * - Thread-local tracking of accessed records per transaction
 * - Temperature updates on abort
 * - Automatic cleanup on commit
 */

#ifndef _MOCC_INTEGRATION_H_
#define _MOCC_INTEGRATION_H_

#include <vector>
#include <atomic>
#include "deptran/mocc/temperature.h"

namespace mocc_integration {

/**
 * Thread-local transaction state for MOCC tracking
 */
struct MoccTxnState {
  std::vector<void*> accessed_records;
  bool is_retry = false;
  uint64_t txn_id = 0;
  
  void clear() {
    accessed_records.clear();
    is_retry = false;
    txn_id = 0;
  }
};

// Thread-local storage for per-transaction MOCC state
extern thread_local MoccTxnState g_mocc_txn_state;

// Global flag to enable/disable MOCC integration
extern std::atomic<bool> g_mocc_enabled;

/**
 * Enable MOCC integration
 */
inline void EnableMocc() {
  g_mocc_enabled.store(true, std::memory_order_release);
}

/**
 * Disable MOCC integration
 */
inline void DisableMocc() {
  g_mocc_enabled.store(false, std::memory_order_release);
}

/**
 * Check if MOCC is enabled
 */
inline bool IsMoccEnabled() {
  return g_mocc_enabled.load(std::memory_order_acquire);
}

/**
 * Begin tracking a new transaction
 * Called from new_txn()
 */
inline void BeginTransaction(uint64_t txn_id) {
  if (!IsMoccEnabled()) return;
  
  g_mocc_txn_state.clear();
  g_mocc_txn_state.txn_id = txn_id;
}

/**
 * Record an access to a record
 * Called from get(), put(), insert() operations
 * 
 * @param key_ptr Pointer to key data (used as record identifier)
 */
inline void RecordAccess(const void* key_ptr) {
  if (!IsMoccEnabled()) return;
  
  // Use key pointer as record identifier for temperature tracking
  g_mocc_txn_state.accessed_records.push_back(const_cast<void*>(key_ptr));
}

/**
 * Handle transaction abort - update temperatures
 * Called from abort_txn()
 */
inline void OnAbort() {
  if (!IsMoccEnabled()) return;
  
  auto& temp_tracker = janus::TemperatureTracker::Instance();
  
  // Increase temperature on all accessed records
  for (void* record : g_mocc_txn_state.accessed_records) {
    temp_tracker.RecordAbort(record, 0);
  }
  
  g_mocc_txn_state.clear();
}

/**
 * Handle transaction commit - cleanup state
 * Called from commit_txn() on success
 */
inline void OnCommit() {
  if (!IsMoccEnabled()) return;
  
  // In MOCC, temperature does NOT decrease on commit
  // Just clear the tracking state
  g_mocc_txn_state.clear();
}

/**
 * Mark current transaction as a retry
 * Called when retrying after abort
 */
inline void MarkRetry() {
  if (!IsMoccEnabled()) return;
  
  g_mocc_txn_state.is_retry = true;
}

/**
 * Check if current transaction is a retry
 */
inline bool IsRetry() {
  if (!IsMoccEnabled()) return false;
  
  return g_mocc_txn_state.is_retry;
}

/**
 * Check temperature level for a record
 */
inline janus::TemperatureLevel GetTemperature(const void* key_ptr) {
  if (!IsMoccEnabled()) return janus::TemperatureLevel::COLD;
  
  return janus::TemperatureTracker::Instance().GetLevel(
      const_cast<void*>(key_ptr), 0);
}

/**
 * Get temperature statistics
 */
inline janus::TemperatureTracker::Stats GetStats() {
  return janus::TemperatureTracker::Instance().GetStats();
}

} // namespace mocc_integration

#endif /* _MOCC_INTEGRATION_H_ */
