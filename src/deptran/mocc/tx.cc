/**
 * MOCC Transaction Implementation
 */

#include "tx.h"

namespace janus {

TxMocc::~TxMocc() {
  // Release any MOCC locks we still hold
  ReleaseMoccLocks();
}

bool TxMocc::ReadColumn(mdb::Row *row,
                        mdb::colid_t col_id,
                        Value *value,
                        int hint_flag) {
  // Check temperature and decide on access strategy
  bool should_lock = ShouldLockPessimistically(row, col_id);
  bool lock_acquired = false;
  
  if (should_lock) {
    // Hot record - acquire shared lock pessimistically
    auto& lock_mgr = MoccLockManager::Instance();
    
    // Check if we already have a lock (from pre-acquisition or previous access)
    if (!HasLockFor(row, col_id)) {
      LockStatus status = lock_mgr.AcquireLock(tid_, row, col_id, 
                                               LockMode::SHARED, true);
      if (status != LockStatus::ACQUIRED) {
        Log_debug("MOCC: Failed to acquire shared lock for read, tx: %" PRIx64, tid_);
        return false;
      }
      lock_acquired = true;
    }
    
    hot_access_count_++;
  }
  
  // Record this access
  RecordAccess(row, col_id, false);
  
  // Track in access history
  access_history_.emplace_back(row, col_id, false, should_lock, lock_acquired);
  
  // Update temperature (increment on access)
  TemperatureTracker::Instance().IncrementTemperature(row, col_id);
  
  // Perform the actual read using parent implementation
  return TxOcc::ReadColumn(row, col_id, value, hint_flag);
}

bool TxMocc::ReadColumns(Row *row,
                         const std::vector<colid_t> &col_ids,
                         std::vector<Value> *values,
                         int hint_flag) {
  // Process each column
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
    // Hot record - acquire exclusive lock pessimistically
    auto& lock_mgr = MoccLockManager::Instance();
    
    // Check if we already have a lock
    if (!HasLockFor(row, col_id)) {
      LockStatus status = lock_mgr.AcquireLock(tid_, row, col_id,
                                               LockMode::EXCLUSIVE, true);
      if (status != LockStatus::ACQUIRED) {
        Log_debug("MOCC: Failed to acquire exclusive lock for write, tx: %" PRIx64, tid_);
        return false;
      }
      lock_acquired = true;
    }
    
    hot_access_count_++;
  }
  
  // Record this access
  RecordAccess(row, col_id, true);
  
  // Track in access history
  access_history_.emplace_back(row, col_id, true, should_lock, lock_acquired);
  
  // Update temperature (writes increase temperature more)
  TemperatureTracker::Instance().IncrementTemperature(row, col_id, 2);
  
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
  if (is_retry_with_locks_) {
    return true;
  }
  
  // Check temperature
  TemperatureLevel level = TemperatureTracker::Instance().GetLevel(row, col_id);
  return level == TemperatureLevel::HOT;
}

std::vector<LockInfo> TxMocc::GetAcquiredLocks() const {
  std::vector<LockInfo> locks;
  
  for (const auto& access : access_history_) {
    if (access.lock_acquired) {
      LockMode mode = access.is_write ? LockMode::EXCLUSIVE : LockMode::SHARED;
      locks.emplace_back(access.row, access.col_id, mode);
    }
  }
  
  return locks;
}

std::vector<LockInfo> TxMocc::GetConflictingRecords() const {
  std::vector<LockInfo> conflicts;
  
  for (const auto& rec : conflict_records_) {
    // For retry, we'll want exclusive locks on conflicting records
    conflicts.emplace_back(rec.first, rec.second, LockMode::EXCLUSIVE);
  }
  
  return conflicts;
}

bool TxMocc::PreAcquireLocks(const std::vector<LockInfo>& locks) {
  auto& lock_mgr = MoccLockManager::Instance();
  
  // Store for reference
  pre_acquired_locks_ = locks;
  
  // Try to acquire all locks
  return lock_mgr.PreAcquireLocks(tid_, locks);
}

void TxMocc::ReleaseMoccLocks() {
  auto& lock_mgr = MoccLockManager::Instance();
  lock_mgr.ReleaseAllLocks(tid_);
}

void TxMocc::RecordAbortOnRecords() {
  auto& temp_tracker = TemperatureTracker::Instance();
  
  // Increase temperature on all accessed records
  for (const auto& access : access_history_) {
    temp_tracker.RecordAbort(access.row, access.col_id);
  }
  
  // Also record conflicts
  for (const auto& rec : conflict_records_) {
    temp_tracker.RecordAbort(rec.first, rec.second);
  }
}

void TxMocc::RecordCommitOnRecords() {
  auto& temp_tracker = TemperatureTracker::Instance();
  
  // Slightly decrease temperature on committed records
  for (const auto& access : access_history_) {
    temp_tracker.RecordCommit(access.row, access.col_id);
  }
}

bool TxMocc::IsHotTransaction() const {
  return hot_access_count_ >= HOT_TXN_THRESHOLD;
}

void TxMocc::RecordAccess(void* row, int col_id, bool is_write) {
  // This is used to track accesses for conflict detection
  // The actual tracking is done in access_history_
  
  if (is_write) {
    // Track for potential conflict reporting
    // Note: conflict_records_ is populated during validation failure
  }
}

bool TxMocc::HasLockFor(void* row, int col_id) const {
  // Check pre-acquired locks
  for (const auto& lock : pre_acquired_locks_) {
    if (lock.row == row && lock.col_id == col_id) {
      return true;
    }
  }
  
  // Check already acquired locks in this transaction
  for (const auto& access : access_history_) {
    if (access.row == row && access.col_id == col_id && access.lock_acquired) {
      return true;
    }
  }
  
  return false;
}

} // namespace janus




