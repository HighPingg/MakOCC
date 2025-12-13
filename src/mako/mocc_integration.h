/**
 * MOCC Integration Layer for Mako/STO
 * 
 * Implements paper-accurate MOCC with custom requirements:
 * - No retries (abort is final, no RLL)
 * - Always restore canonical mode
 * - Logical clock (clock, node_id) ordering
 * - Wait-die deadlock prevention
 * 
 * Key Components:
 * - Temperature tracking (per-page, probabilistic)
 * - Current Lock List (CLL) for canonical mode
 * - Lock acquisition with wait-die
 */

#ifndef _MOCC_INTEGRATION_H_
#define _MOCC_INTEGRATION_H_

#include <vector>
#include <map>
#include <atomic>
#include <algorithm>
#include "deptran/mocc/temperature.h"
#include "deptran/mocc/mocc_lock.h"
#include "deptran/mocc/logical_clock.h"

namespace mocc_integration {

/**
 * ReadSetEntry tracks a record read and its TID for OCC validation
 */
struct ReadSetEntry {
  void* record;
  uint64_t observed_tid;  // TID at time of read
  
  ReadSetEntry(void* r, uint64_t tid) : record(r), observed_tid(tid) {}
};

/**
 * CLLEntry tracks a lock held by this transaction (Current Lock List)
 */
struct CLLEntry {
  void* record;
  int col_id;
  janus::LockMode mode;
  
  CLLEntry(void* r, int c, janus::LockMode m) : record(r), col_id(c), mode(m) {}
  
  // Ordering for canonical mode (by record address)
  bool operator<(const CLLEntry& other) const {
    if (record != other.record) return record < other.record;
    return col_id < other.col_id;
  }
};

/**
 * Thread-local transaction state for MOCC
 */
struct MoccTxnState {
  // Transaction identity
  uint64_t txn_id = 0;
  janus::LogicalTimestamp timestamp;
  
  // Read/write sets for OCC validation
  std::vector<ReadSetEntry> read_set;
  std::vector<void*> write_set;
  
  // Current Lock List - sorted for canonical mode
  std::vector<CLLEntry> cll;
  
  void Clear() {
    txn_id = 0;
    timestamp = janus::LogicalTimestamp();
    read_set.clear();
    write_set.clear();
    cll.clear();
  }
};

// Thread-local storage for per-transaction MOCC state
extern thread_local MoccTxnState g_mocc_txn_state;

// Global flag to enable/disable MOCC integration
extern std::atomic<bool> g_mocc_enabled;

// ============================================================================
// Enable/Disable
// ============================================================================

inline void EnableMocc() {
  g_mocc_enabled.store(true, std::memory_order_release);
}

inline void DisableMocc() {
  g_mocc_enabled.store(false, std::memory_order_release);
}

inline bool IsMoccEnabled() {
  return g_mocc_enabled.load(std::memory_order_acquire);
}

// ============================================================================
// Temperature Helpers
// ============================================================================

inline bool IsHot(void* record) {
  if (!IsMoccEnabled()) return false;
  return janus::TemperatureTracker::Instance().GetLevel(record, 0) 
         == janus::TemperatureLevel::HOT;
}

inline void IncreaseTemperature(void* record) {
  janus::TemperatureTracker::Instance().RecordAbort(record, 0);
}

// ============================================================================
// Canonical Mode Helpers
// ============================================================================

/**
 * Check if CLL contains a lock for this record
 */
inline bool HasLock(void* record, int col_id = 0) {
  auto& cll = g_mocc_txn_state.cll;
  for (const auto& entry : cll) {
    if (entry.record == record && entry.col_id == col_id) {
      return true;
    }
  }
  return false;
}

/**
 * Get locks in CLL that violate canonical order if we acquire 'record'
 * 
 * Canonical mode: All held locks must be ordered BEFORE the new lock.
 * Violations are locks in CLL with address >= record.
 */
inline std::vector<CLLEntry> GetCanonicalViolations(void* record, int col_id = 0) {
  std::vector<CLLEntry> violations;
  for (const auto& entry : g_mocc_txn_state.cll) {
    // Violation: existing lock is NOT less than new lock
    if (entry.record > record || 
        (entry.record == record && entry.col_id >= col_id)) {
      violations.push_back(entry);
    }
  }
  return violations;
}

/**
 * Release a specific lock and remove from CLL
 */
inline void ReleaseLockFromCLL(void* record, int col_id = 0) {
  auto& state = g_mocc_txn_state;
  
  // Release from lock manager
  janus::MoccLockManager::Instance().ReleaseLock(state.txn_id, record, col_id);
  
  // Remove from CLL
  auto& cll = state.cll;
  cll.erase(std::remove_if(cll.begin(), cll.end(),
    [record, col_id](const CLLEntry& e) {
      return e.record == record && e.col_id == col_id;
    }), cll.end());
}

/**
 * Restore canonical mode by releasing all violating locks
 * 
 * ALWAYS restore (your requirement) - no "alternative locking" path
 */
inline void RestoreCanonicalMode(void* target_record, int target_col = 0) {
  auto violations = GetCanonicalViolations(target_record, target_col);
  
  // Release all violating locks
  for (const auto& v : violations) {
    ReleaseLockFromCLL(v.record, v.col_id);
  }
}

// ============================================================================
// Lock Acquisition (with canonical mode)
// ============================================================================

/**
 * Acquire a lock on a record using MOCC protocol
 * 
 * Steps:
 * 1. Check if record is HOT (cold records use OCC, no lock)
 * 2. Check canonical mode - release violating locks if needed
 * 3. Acquire lock using wait-die
 * 
 * @param record Record to lock
 * @param col_id Column ID (0 for entire record)
 * @param mode SHARED (read) or EXCLUSIVE (write)
 * @return true if lock acquired, false if must abort (wait-die: die)
 */
inline bool Lock(void* record, janus::LockMode mode, int col_id = 0) {
  if (!IsMoccEnabled()) return true;
  
  auto& state = g_mocc_txn_state;
  
  // Check if we already hold a compatible lock
  for (const auto& entry : state.cll) {
    if (entry.record == record && entry.col_id == col_id) {
      if (entry.mode == janus::LockMode::EXCLUSIVE) {
        return true;  // Already have exclusive
      }
      if (mode == janus::LockMode::SHARED) {
        return true;  // Already have shared, requesting shared
      }
      // Have shared, need exclusive - will try to upgrade
    }
  }
  
  // ALWAYS restore canonical mode (your requirement)
  RestoreCanonicalMode(record, col_id);
  
  // Now in canonical mode - acquire lock with wait-die
  auto& lock_mgr = janus::MoccLockManager::Instance();
  
  // Use blocking=true for wait (older txns wait for younger)
  auto status = lock_mgr.AcquireLock(state.txn_id, record, col_id, mode, true);
  
  if (status == janus::LockStatus::DENIED) {
    // Wait-die: We are younger, must abort (no retry per your requirement)
    return false;
  }
  
  // Lock acquired - add to CLL in sorted order
  CLLEntry new_entry(record, col_id, mode);
  auto insert_pos = std::lower_bound(state.cll.begin(), state.cll.end(), new_entry);
  state.cll.insert(insert_pos, new_entry);
  
  return true;
}

// ============================================================================
// Transaction Lifecycle
// ============================================================================

/**
 * Begin a new transaction
 * Called from new_txn()
 */
inline void BeginTransaction(uint64_t txn_id) {
  if (!IsMoccEnabled()) return;
  
  auto& state = g_mocc_txn_state;
  state.Clear();
  state.txn_id = txn_id;
  state.timestamp = janus::LogicalClockManager::Instance().GetTimestamp();
  
  // Register with lock manager
  janus::MoccLockManager::Instance().RegisterTransaction(txn_id, state.timestamp);
}

/**
 * Handle a read operation
 * - Record in read set for OCC validation
 * - If HOT, acquire SHARED lock
 * 
 * @param record Record being read
 * @param tid Current TID of the record (for validation)
 * @return true if successful, false if must abort
 */
inline bool OnRead(void* record, uint64_t tid) {
  if (!IsMoccEnabled()) return true;
  
  auto& state = g_mocc_txn_state;
  
  // Add to read set
  state.read_set.emplace_back(record, tid);
  
  // If HOT, acquire read lock
  if (IsHot(record)) {
    if (!Lock(record, janus::LockMode::SHARED)) {
      return false;  // Must abort (wait-die: die)
    }
  }
  
  return true;
}

/**
 * Handle a write operation
 * - Record in write set
 * - If HOT, acquire EXCLUSIVE lock
 * 
 * @param record Record being written
 * @param tid Current TID (for read-modify-write validation)
 * @return true if successful, false if must abort
 */
inline bool OnWrite(void* record, uint64_t tid) {
  if (!IsMoccEnabled()) return true;
  
  auto& state = g_mocc_txn_state;
  
  // Add to read set (for validation) and write set
  state.read_set.emplace_back(record, tid);
  state.write_set.push_back(record);
  
  // If HOT, acquire write lock
  if (IsHot(record)) {
    if (!Lock(record, janus::LockMode::EXCLUSIVE)) {
      return false;  // Must abort
    }
  }
  
  return true;
}

/**
 * Pre-commit: Lock write set and validate reads
 * 
 * Steps:
 * 1. Sort write set for canonical ordering
 * 2. Acquire EXCLUSIVE locks on all writes
 * 3. Validate reads (OCC)
 * 
 * @param get_tid Function to get current TID of a record
 * @return true if can commit, false if must abort
 */
template<typename GetTidFunc>
inline bool OnPreCommit(GetTidFunc get_tid) {
  if (!IsMoccEnabled()) return true;
  
  auto& state = g_mocc_txn_state;
  
  // Sort write set for canonical lock ordering
  std::sort(state.write_set.begin(), state.write_set.end());
  
  // Acquire EXCLUSIVE locks on all writes
  for (void* record : state.write_set) {
    if (!Lock(record, janus::LockMode::EXCLUSIVE)) {
      return false;  // Wait-die: must abort
    }
  }
  
  // OCC validation: check all reads
  for (const auto& entry : state.read_set) {
    uint64_t current_tid = get_tid(entry.record);
    if (current_tid != entry.observed_tid) {
      // Clobbered read - increase temperature
      IncreaseTemperature(entry.record);
      return false;  // Must abort
    }
  }
  
  return true;  // Can commit
}

/**
 * Commit: Release all locks and cleanup
 */
inline void OnCommit() {
  if (!IsMoccEnabled()) return;
  
  auto& state = g_mocc_txn_state;
  
  // Release all locks
  janus::MoccLockManager::Instance().ReleaseAllLocks(state.txn_id);
  janus::MoccLockManager::Instance().UnregisterTransaction(state.txn_id);
  
  // Clear state
  state.Clear();
}

/**
 * Abort: Update temperatures, release locks, NO RETRY
 * 
 * Per your requirement: abort is final, no RLL construction
 */
inline void OnAbort() {
  if (!IsMoccEnabled()) return;
  
  auto& state = g_mocc_txn_state;
  
  // Increase temperature on all accessed records
  for (const auto& entry : state.read_set) {
    IncreaseTemperature(entry.record);
  }
  
  // Release all locks
  janus::MoccLockManager::Instance().ReleaseAllLocks(state.txn_id);
  janus::MoccLockManager::Instance().UnregisterTransaction(state.txn_id);
  
  // Clear state - NO RLL (your requirement)
  state.Clear();
}

// ============================================================================
// Legacy API (for backward compatibility)
// ============================================================================

inline void RecordAccess(const void* key_ptr) {
  // Legacy: just track in read set with TID=0 (no validation)
  if (!IsMoccEnabled()) return;
  g_mocc_txn_state.read_set.emplace_back(const_cast<void*>(key_ptr), 0);
}

inline janus::TemperatureLevel GetTemperature(const void* key_ptr) {
  if (!IsMoccEnabled()) return janus::TemperatureLevel::COLD;
  return janus::TemperatureTracker::Instance().GetLevel(
      const_cast<void*>(key_ptr), 0);
}

inline janus::TemperatureTracker::Stats GetStats() {
  return janus::TemperatureTracker::Instance().GetStats();
}

inline void MarkRetry() {
  // No-op: no retries per your requirement
}

inline bool IsRetry() {
  return false;  // No retries
}

} // namespace mocc_integration

#endif /* _MOCC_INTEGRATION_H_ */
