/**
 * MOCC Locking Data Structure Implementation
 */

#include "mocc_lock.h"

namespace janus {

// ============================================================================
// RecordLock Implementation
// ============================================================================

RecordLock::RecordLock() = default;

bool RecordLock::CanGrant(LockMode mode, txnid_t txn_id) const {
  if (mode == LockMode::SHARED) {
    // Shared lock can be granted if:
    // - No exclusive holder, OR
    // - We already hold the exclusive lock (upgrade scenario)
    return exclusive_holder_ == 0 || exclusive_holder_ == txn_id;
  } else if (mode == LockMode::EXCLUSIVE) {
    // Exclusive lock can be granted if:
    // - No one holds any lock, OR
    // - We are the only holder (upgrade from shared)
    if (exclusive_holder_ != 0) {
      return exclusive_holder_ == txn_id;
    }
    if (shared_holders_.empty()) {
      return true;
    }
    // Check if we're the only shared holder (upgrade case)
    return shared_holders_.size() == 1 && 
           shared_holders_.find(txn_id) != shared_holders_.end();
  }
  return false;
}

LockStatus RecordLock::Acquire(txnid_t txn_id, LockMode mode, bool blocking) {
  std::unique_lock<std::mutex> lock(mutex_);
  
  // Check if we already hold a compatible lock
  if (mode == LockMode::SHARED && shared_holders_.count(txn_id)) {
    return LockStatus::ACQUIRED;  // Already have shared
  }
  if (mode == LockMode::EXCLUSIVE && exclusive_holder_ == txn_id) {
    return LockStatus::ACQUIRED;  // Already have exclusive
  }
  
  // Try to acquire immediately
  if (CanGrant(mode, txn_id)) {
    if (mode == LockMode::SHARED) {
      shared_holders_.insert(txn_id);
    } else {
      // If upgrading from shared, remove from shared holders
      shared_holders_.erase(txn_id);
      exclusive_holder_ = txn_id;
    }
    return LockStatus::ACQUIRED;
  }
  
  // Cannot acquire immediately
  if (!blocking) {
    return LockStatus::DENIED;
  }
  
  // Add to wait queue and wait
  wait_queue_.push(LockRequest(txn_id, mode));
  
  cv_.wait(lock, [this, txn_id, mode]() {
    return CanGrant(mode, txn_id);
  });
  
  // Grant the lock
  if (mode == LockMode::SHARED) {
    shared_holders_.insert(txn_id);
  } else {
    shared_holders_.erase(txn_id);
    exclusive_holder_ = txn_id;
  }
  
  // Remove from wait queue
  // Note: This is a simplified implementation
  
  return LockStatus::ACQUIRED;
}

bool RecordLock::TryAcquire(txnid_t txn_id, LockMode mode) {
  return Acquire(txn_id, mode, false) == LockStatus::ACQUIRED;
}

bool RecordLock::Release(txnid_t txn_id) {
  std::lock_guard<std::mutex> lock(mutex_);
  
  bool released = false;
  
  if (exclusive_holder_ == txn_id) {
    exclusive_holder_ = 0;
    released = true;
  }
  
  if (shared_holders_.erase(txn_id) > 0) {
    released = true;
  }
  
  if (released) {
    // Wake up waiters
    GrantWaiters();
    cv_.notify_all();
  }
  
  return released;
}

void RecordLock::GrantWaiters() {
  // Process wait queue - grant as many compatible requests as possible
  // This is called while holding the mutex
  
  if (wait_queue_.empty()) {
    return;
  }
  
  // Simple approach: just notify all and let them re-check
  // More sophisticated: directly grant and update state
}

bool RecordLock::IsHeldBy(txnid_t txn_id) const {
  std::lock_guard<std::mutex> lock(mutex_);
  return exclusive_holder_ == txn_id || 
         shared_holders_.find(txn_id) != shared_holders_.end();
}

bool RecordLock::IsExclusivelyHeld() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return exclusive_holder_ != 0;
}

txnid_t RecordLock::GetExclusiveHolder() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return exclusive_holder_;
}

std::vector<txnid_t> RecordLock::GetSharedHolders() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return std::vector<txnid_t>(shared_holders_.begin(), shared_holders_.end());
}

size_t RecordLock::GetWaiterCount() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return wait_queue_.size();
}

bool RecordLock::IsFree() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return exclusive_holder_ == 0 && shared_holders_.empty();
}

// ============================================================================
// MoccLockManager Implementation
// ============================================================================

RecordLock& MoccLockManager::GetOrCreateLock(void* row, int col_id) {
  RowColKey key{row, col_id};
  
  // Try with shared lock first
  {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    auto it = locks_.find(key);
    if (it != locks_.end()) {
      return *it->second;
    }
  }
  
  // Need to create - use exclusive lock
  std::unique_lock<std::shared_mutex> lock(mutex_);
  
  // Double-check
  auto it = locks_.find(key);
  if (it != locks_.end()) {
    return *it->second;
  }
  
  // Create new lock
  auto new_lock = std::make_unique<RecordLock>();
  auto& ref = *new_lock;
  locks_[key] = std::move(new_lock);
  return ref;
}

LockStatus MoccLockManager::AcquireLock(txnid_t txn_id, void* row, int col_id,
                                        LockMode mode, bool force) {
  // Check if we should use pessimistic locking
  if (!force && !ShouldUsePessimisticLock(row, col_id)) {
    // For cold records, we don't actually acquire locks
    // Just return ACQUIRED to indicate optimistic path
    return LockStatus::ACQUIRED;
  }
  
  RecordLock& lock = GetOrCreateLock(row, col_id);
  LockStatus status = lock.Acquire(txn_id, mode, true);  // blocking
  
  if (status == LockStatus::ACQUIRED) {
    // Track this lock for the transaction
    std::lock_guard<std::mutex> guard(txn_locks_mutex_);
    txn_held_locks_[txn_id].push_back(RowColKey{row, col_id});
  }
  
  return status;
}

bool MoccLockManager::TryAcquireLock(txnid_t txn_id, void* row, int col_id, LockMode mode) {
  RecordLock& lock = GetOrCreateLock(row, col_id);
  bool acquired = lock.TryAcquire(txn_id, mode);
  
  if (acquired) {
    std::lock_guard<std::mutex> guard(txn_locks_mutex_);
    txn_held_locks_[txn_id].push_back(RowColKey{row, col_id});
  }
  
  return acquired;
}

bool MoccLockManager::ReleaseLock(txnid_t txn_id, void* row, int col_id) {
  RowColKey key{row, col_id};
  
  std::shared_lock<std::shared_mutex> lock(mutex_);
  auto it = locks_.find(key);
  if (it == locks_.end()) {
    return false;
  }
  
  bool released = it->second->Release(txn_id);
  
  if (released) {
    // Remove from tracking
    std::lock_guard<std::mutex> guard(txn_locks_mutex_);
    auto txn_it = txn_held_locks_.find(txn_id);
    if (txn_it != txn_held_locks_.end()) {
      auto& locks_vec = txn_it->second;
      locks_vec.erase(
        std::remove(locks_vec.begin(), locks_vec.end(), key),
        locks_vec.end()
      );
      if (locks_vec.empty()) {
        txn_held_locks_.erase(txn_it);
      }
    }
  }
  
  return released;
}

void MoccLockManager::ReleaseAllLocks(txnid_t txn_id) {
  std::vector<RowColKey> locks_to_release;
  
  // Get list of locks held by this transaction
  {
    std::lock_guard<std::mutex> guard(txn_locks_mutex_);
    auto it = txn_held_locks_.find(txn_id);
    if (it != txn_held_locks_.end()) {
      locks_to_release = it->second;
      txn_held_locks_.erase(it);
    }
  }
  
  // Release each lock
  std::shared_lock<std::shared_mutex> lock(mutex_);
  for (const auto& key : locks_to_release) {
    auto it = locks_.find(key);
    if (it != locks_.end()) {
      it->second->Release(txn_id);
    }
  }
}

bool MoccLockManager::HoldsLock(txnid_t txn_id, void* row, int col_id) const {
  RowColKey key{row, col_id};
  
  std::shared_lock<std::shared_mutex> lock(mutex_);
  auto it = locks_.find(key);
  if (it == locks_.end()) {
    return false;
  }
  return it->second->IsHeldBy(txn_id);
}

bool MoccLockManager::ShouldUsePessimisticLock(void* row, int col_id) const {
  TemperatureLevel level = TemperatureTracker::Instance().GetLevel(row, col_id);
  return level == TemperatureLevel::HOT;
}

std::vector<LockInfo> MoccLockManager::GetHeldLocks(txnid_t txn_id) const {
  std::vector<LockInfo> result;
  
  std::lock_guard<std::mutex> guard(txn_locks_mutex_);
  auto it = txn_held_locks_.find(txn_id);
  if (it != txn_held_locks_.end()) {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    for (const auto& key : it->second) {
      auto lock_it = locks_.find(key);
      if (lock_it != locks_.end()) {
        LockMode mode = lock_it->second->IsExclusivelyHeld() ? 
                        LockMode::EXCLUSIVE : LockMode::SHARED;
        result.emplace_back(key.row_ptr, key.col_id, mode);
      }
    }
  }
  
  return result;
}

std::vector<LockInfo> MoccLockManager::GetRequiredLocks(
    const std::vector<std::pair<void*, int>>& records, bool write) const {
  std::vector<LockInfo> result;
  LockMode mode = write ? LockMode::EXCLUSIVE : LockMode::SHARED;
  
  for (const auto& rec : records) {
    result.emplace_back(rec.first, rec.second, mode);
  }
  
  return result;
}

bool MoccLockManager::PreAcquireLocks(txnid_t txn_id, const std::vector<LockInfo>& locks) {
  // Sort locks to prevent deadlock (by pointer address)
  std::vector<LockInfo> sorted_locks = locks;
  std::sort(sorted_locks.begin(), sorted_locks.end(),
            [](const LockInfo& a, const LockInfo& b) {
              if (a.row != b.row) return a.row < b.row;
              return a.col_id < b.col_id;
            });
  
  // Try to acquire all locks
  std::vector<RowColKey> acquired;
  
  for (const auto& lock_info : sorted_locks) {
    RecordLock& lock = GetOrCreateLock(lock_info.row, lock_info.col_id);
    if (!lock.TryAcquire(txn_id, lock_info.mode)) {
      // Failed to acquire - rollback all acquired locks
      for (const auto& key : acquired) {
        auto it = locks_.find(key);
        if (it != locks_.end()) {
          it->second->Release(txn_id);
        }
      }
      return false;
    }
    acquired.push_back(RowColKey{lock_info.row, lock_info.col_id});
  }
  
  // All locks acquired successfully - track them
  {
    std::lock_guard<std::mutex> guard(txn_locks_mutex_);
    auto& txn_locks = txn_held_locks_[txn_id];
    txn_locks.insert(txn_locks.end(), acquired.begin(), acquired.end());
  }
  
  return true;
}

void MoccLockManager::Clear() {
  std::unique_lock<std::shared_mutex> lock(mutex_);
  std::lock_guard<std::mutex> guard(txn_locks_mutex_);
  
  locks_.clear();
  txn_held_locks_.clear();
}

MoccLockManager::Stats MoccLockManager::GetStats() const {
  Stats stats{0, 0, 0};
  
  std::shared_lock<std::shared_mutex> lock(mutex_);
  stats.total_locks = locks_.size();
  
  for (const auto& pair : locks_) {
    if (!pair.second->IsFree()) {
      stats.held_locks++;
    }
    stats.waiting_requests += pair.second->GetWaiterCount();
  }
  
  return stats;
}

} // namespace janus




