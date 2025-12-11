/**
 * MOCC Transaction Implementation
 * 
 * Implements:
 * - Temperature-aware read/write with hot record locking
 * - Canonical mode with MANDATORY restoration (never skip)
 * - Wait-Die deadlock prevention
 * - Proper lock cleanup on commit/abort
 */

#include "tx.h"
#include <algorithm>

namespace janus {

TxMocc::~TxMocc() {
  // Release any MOCC locks we still hold
  ReleaseMoccLocks();
}

void TxMocc::InitializeTimestamp(uint32_t coordinator_id) {
  // Get timestamp from logical clock manager
  auto& clock_mgr = LogicalClockManager::Instance();
  if (clock_mgr.GetNodeId() == 0) {
    // Initialize with coordinator ID if not set
    clock_mgr.Initialize(coordinator_id);
  }
  
  timestamp_ = clock_mgr.GetTimestamp();
  
  // Register with lock manager
  MoccLockManager::Instance().RegisterTransaction(tid_, timestamp_);
}

bool TxMocc::ReadColumn(mdb::Row *row,
                        mdb::colid_t col_id,
                        Value *value,
                        int hint_flag) {
  // Check temperature and decide on access strategy
  bool should_lock = ShouldLockPessimistically(row, col_id);
  bool lock_acquired = false;
  
  if (should_lock) {
    // Hot record - acquire shared lock with canonical mode
    if (!AcquireLockCanonical(row, col_id, LockMode::SHARED)) {
      Log_debug("MOCC: Failed to acquire lock for read in canonical mode, tx: %" PRIx64, tid_);
      return false;
    }
    lock_acquired = true;
    hot_access_count_++;
  }
  
  // Record this access
  RecordAccess(row, col_id, false);
  
  // Track in access history
  access_history_.emplace_back(row, col_id, false, should_lock, lock_acquired);
  
  // NOTE: Temperature increment removed - MOCC only increases temp on abort
  
  // Perform the actual read using parent implementation
  return TxOcc::ReadColumn(row, col_id, value, hint_flag);
}

bool TxMocc::ReadColumns(Row *row,
                         const std::vector<colid_t> &col_ids,
                         std::vector<Value> *values,
                         int hint_flag) {
  values->resize(col_ids.size());
  for (size_t i = 0; i < col_ids.size(); i++) {
    if (!ReadColumn(row, col_ids[i], &(*values)[i], hint_flag)) {
      return false;
    }
  }
  return true;
}

bool TxMocc::WriteColumn(Row *row,
                         colid_t col_id,
                         const Value &value,
                         int hint_flag) {
  // Check temperature and decide on access strategy
  bool should_lock = ShouldLockPessimistically(row, col_id);
  bool lock_acquired = false;
  
  if (should_lock) {
    // Hot record - acquire exclusive lock with canonical mode
    if (!AcquireLockCanonical(row, col_id, LockMode::EXCLUSIVE)) {
      Log_debug("MOCC: Failed to acquire lock for write in canonical mode, tx: %" PRIx64, tid_);
      return false;
    }
    lock_acquired = true;
    hot_access_count_++;
  }
  
  // Record this access
  RecordAccess(row, col_id, true);
  
  // Track in access history
  access_history_.emplace_back(row, col_id, true, should_lock, lock_acquired);
  
  // NOTE: Temperature increment removed - MOCC only increases temp on abort
  
  // Perform the actual write using parent implementation
  return TxOcc::WriteColumn(row, col_id, value, hint_flag);
}

bool TxMocc::WriteColumns(Row *row,
                          const std::vector<colid_t> &col_ids,
                          const std::vector<Value> &values,
                          int hint_flag) {
  verify(col_ids.size() == values.size());
  for (size_t i = 0; i < col_ids.size(); i++) {
    if (!WriteColumn(row, col_ids[i], values[i], hint_flag)) {
      return false;
    }
  }
  return true;
}

bool TxMocc::ShouldLockPessimistically(void* row, int col_id) const {
  // If this is a retry with pre-acquired locks, use pessimistic for all
  // (Note: with no-retry requirement, this won't normally happen)
  if (is_retry_with_locks_) {
    return true;
  }
  
  // Check temperature
  TemperatureLevel level = TemperatureTracker::Instance().GetLevel(row, col_id);
  return level == TemperatureLevel::HOT;
}

bool TxMocc::AcquireLockCanonical(void* row, int col_id, LockMode mode) {
  // Check if we already have this lock
  if (HasLockFor(row, col_id)) {
    LockMode held_mode = GetHeldLockMode(row, col_id);
    
    // Check if upgrade needed
    if (mode == LockMode::EXCLUSIVE && held_mode == LockMode::SHARED) {
      // Need to upgrade - this might require releasing some locks
      RemoveFromCLL(row, col_id);
      MoccLockManager::Instance().ReleaseLock(tid_, row, col_id);
      // Fall through to acquire exclusive
    } else {
      // Already have sufficient lock
      return true;
    }
  }
  
  // MOCC Algorithm 1 (lines 23-36): Canonical mode lock acquisition
  // Check for violations (locks ordered after the new lock that we already hold)
  std::vector<CurrentLockEntry> violations = GetCanonicalViolations(row, col_id);
  
  // User requirement: ALWAYS restore canonical mode (never skip)
  if (!violations.empty()) {
    // Restore canonical mode by releasing violating locks
    RestoreCanonicalMode(violations);
  }
  
  // Now we're in canonical mode - acquire lock unconditionally (blocking)
  auto& lock_mgr = MoccLockManager::Instance();
  LockStatus status = lock_mgr.AcquireLock(tid_, row, col_id, mode, true);
  
  if (status == LockStatus::ACQUIRED) {
    AddToCLL(row, col_id, mode);
    return true;
  } else if (status == LockStatus::DENIED) {
    // Wait-Die: we must abort (younger transaction)
    Log_debug("MOCC: Wait-Die abort for tx %" PRIx64, tid_);
    return false;
  }
  
  // Should not reach here in blocking mode
  return false;
}

std::vector<CurrentLockEntry> TxMocc::GetCanonicalViolations(void* new_row, int new_col_id) {
  std::vector<CurrentLockEntry> violations;
  
  // A lock l is a violation if l >= new_lock (ordered after new lock)
  // Canonical ordering: by row pointer, then column
  for (const auto& entry : current_lock_list_) {
    if (entry.row > new_row || 
        (entry.row == new_row && entry.col_id >= new_col_id)) {
      violations.push_back(entry);
    }
  }
  
  return violations;
}

void TxMocc::RestoreCanonicalMode(const std::vector<CurrentLockEntry>& violations) {
  // MOCC Algorithm 1 (lines 30-32): Release violating locks
  // User requirement: ALWAYS restore canonical mode
  
  auto& lock_mgr = MoccLockManager::Instance();
  
  for (const auto& entry : violations) {
    Log_debug("MOCC: Releasing lock to restore canonical mode, tx: %" PRIx64 
              ", row: %p, col: %d", tid_, entry.row, entry.col_id);
    
    // Release the lock
    lock_mgr.ReleaseLock(tid_, entry.row, entry.col_id);
    
    // Remove from CLL
    RemoveFromCLL(entry.row, entry.col_id);
  }
}

void TxMocc::AddToCLL(void* row, int col_id, LockMode mode) {
  CurrentLockEntry entry(row, col_id, mode);
  
  // Insert in sorted order (canonical ordering)
  auto pos = std::lower_bound(current_lock_list_.begin(), 
                               current_lock_list_.end(), entry);
  current_lock_list_.insert(pos, entry);
}

void TxMocc::RemoveFromCLL(void* row, int col_id) {
  current_lock_list_.erase(
    std::remove_if(current_lock_list_.begin(), current_lock_list_.end(),
                   [row, col_id](const CurrentLockEntry& e) {
                     return e.row == row && e.col_id == col_id;
                   }),
    current_lock_list_.end()
  );
}

bool TxMocc::HasLockFor(void* row, int col_id) const {
  // Check CLL
  for (const auto& entry : current_lock_list_) {
    if (entry.row == row && entry.col_id == col_id) {
      return true;
    }
  }
  
  // Check pre-acquired locks (for retry mode, though we don't use retry)
  for (const auto& lock : pre_acquired_locks_) {
    if (lock.row == row && lock.col_id == col_id) {
      return true;
    }
  }
  
  return false;
}

LockMode TxMocc::GetHeldLockMode(void* row, int col_id) const {
  for (const auto& entry : current_lock_list_) {
    if (entry.row == row && entry.col_id == col_id) {
      return entry.mode;
    }
  }
  return LockMode::NONE;
}

std::vector<LockInfo> TxMocc::GetAcquiredLocks() const {
  std::vector<LockInfo> locks;
  
  for (const auto& entry : current_lock_list_) {
    locks.emplace_back(entry.row, entry.col_id, entry.mode);
  }
  
  return locks;
}

std::vector<LockInfo> TxMocc::GetConflictingRecords() const {
  std::vector<LockInfo> conflicts;
  
  for (const auto& rec : conflict_records_) {
    // For conflict records, request exclusive locks
    conflicts.emplace_back(rec.first, rec.second, LockMode::EXCLUSIVE);
  }
  
  return conflicts;
}

void TxMocc::AddConflictRecord(void* row, int col_id) {
  conflict_records_.emplace_back(row, col_id);
}

bool TxMocc::PreAcquireLocks(const std::vector<LockInfo>& locks) {
  // Sort locks for canonical ordering
  std::vector<LockInfo> sorted_locks = locks;
  std::sort(sorted_locks.begin(), sorted_locks.end());
  
  pre_acquired_locks_ = sorted_locks;
  
  // Acquire each lock in order
  auto& lock_mgr = MoccLockManager::Instance();
  return lock_mgr.PreAcquireLocks(tid_, sorted_locks);
}

void TxMocc::ReleaseMoccLocks() {
  // Release all locks in CLL
  auto& lock_mgr = MoccLockManager::Instance();
  
  for (const auto& entry : current_lock_list_) {
    lock_mgr.ReleaseLock(tid_, entry.row, entry.col_id);
  }
  current_lock_list_.clear();
  
  // Release pre-acquired locks if any
  for (const auto& lock : pre_acquired_locks_) {
    lock_mgr.ReleaseLock(tid_, lock.row, lock.col_id);
  }
  pre_acquired_locks_.clear();
  
  // Unregister transaction
  lock_mgr.UnregisterTransaction(tid_);
}

void TxMocc::RecordAbortOnRecords() {
  auto& temp_tracker = TemperatureTracker::Instance();
  
  // Increase temperature on all accessed records (MOCC: only on abort)
  for (const auto& access : access_history_) {
    temp_tracker.RecordAbort(access.row, access.col_id);
  }
  
  // Also record conflicts
  for (const auto& rec : conflict_records_) {
    temp_tracker.RecordAbort(rec.first, rec.second);
  }
}

void TxMocc::RecordCommitOnRecords() {
  // MOCC: Temperature does NOT decrease on commit
  // Temperature only increases on abort and decreases via periodic reset
  // This method is kept for API compatibility but does nothing
}

bool TxMocc::IsHotTransaction() const {
  return hot_access_count_ >= HOT_TXN_THRESHOLD;
}

void TxMocc::RecordAccess(void* row, int col_id, bool is_write) {
  // Track access for conflict detection and temperature update
  // The actual tracking is done in access_history_
  (void)row;
  (void)col_id;
  (void)is_write;
}

} // namespace janus
