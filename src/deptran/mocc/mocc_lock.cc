/**
 * MOCC Lock Manager Implementation with Wait-Die Deadlock Prevention
 * 
 * Wait-Die Protocol:
 * - Older transaction (lower timestamp) WAITS for younger holder
 * - Younger transaction (higher timestamp) DIES (aborts) if blocked by older holder
 */

#include "mocc_lock.h"
#include <algorithm>

namespace janus {

// ============================================================================
// RecordLock Implementation
// ============================================================================

RecordLock::RecordLock() {}

bool RecordLock::CanGrant(LockMode mode, txnid_t txn_id) const {
  if (holders_.empty()) {
    return true;  // No holders, can grant
  }
  
  // Check if this transaction already holds a compatible lock
  for (const auto& holder : holders_) {
    if (holder.txn_id == txn_id) {
      // Already hold a lock - check compatibility
      if (holder.mode == LockMode::EXCLUSIVE) {
        return true;  // Already have exclusive
      }
      if (mode == LockMode::SHARED) {
        return true;  // Already have shared, requesting shared
      }
      // Have shared, requesting exclusive - upgrade needed
      // Can upgrade only if we're the only holder
      return holders_.size() == 1;
    }
  }
  
  // Check compatibility with existing holders
  if (mode == LockMode::SHARED) {
    // Shared can be granted if no exclusive holders
    for (const auto& holder : holders_) {
      if (holder.mode == LockMode::EXCLUSIVE) {
        return false;
      }
    }
    return true;
  } else {
    // Exclusive can only be granted if no holders
    return false;
  }
}

bool RecordLock::ShouldWait(const LogicalTimestamp& requester_ts) const {
  // Wait-Die: Older (smaller timestamp) waits, younger dies
  // Returns true if requester should wait (is older than all holders)
  // Returns false if requester should abort (is younger than any holder)
  
  for (const auto& holder : holders_) {
    if (requester_ts > holder.timestamp) {
      // Requester is younger than this holder - must abort (die)
      return false;
    }
  }
  // Requester is older than all holders - can wait
  return true;
}

LogicalTimestamp RecordLock::FindOldestHolder() const {
  if (holders_.empty()) {
    return LogicalTimestamp();
  }
  
  LogicalTimestamp oldest = holders_[0].timestamp;
  for (const auto& holder : holders_) {
    if (holder.timestamp < oldest) {
      oldest = holder.timestamp;
    }
  }
  return oldest;
}

LockStatus RecordLock::Acquire(txnid_t txn_id, LogicalTimestamp timestamp,
                                LockMode mode, bool blocking) {
  std::unique_lock<std::mutex> lock(mutex_);
  
  // Check if we can grant immediately
  if (CanGrant(mode, txn_id)) {
    // Check if upgrading
    bool found = false;
    for (auto& holder : holders_) {
      if (holder.txn_id == txn_id) {
        holder.mode = mode;  // Upgrade
        found = true;
        break;
      }
    }
    if (!found) {
      holders_.emplace_back(txn_id, timestamp, mode);
    }
    return LockStatus::ACQUIRED;
  }
  
  // Cannot grant immediately - apply Wait-Die
  if (!ShouldWait(timestamp)) {
    // Younger transaction - must abort (die)
    return LockStatus::DENIED;
  }
  
  // Older transaction - can wait
  if (!blocking) {
    // Non-blocking mode - return waiting status but don't actually wait
    return LockStatus::WAITING;
  }
  
  // Create waiter and add to wait list
  auto waiter = std::make_shared<LockWaiter>(txn_id, timestamp, mode);
  
  // Insert in timestamp order for fairness
  auto insert_pos = waiters_.begin();
  for (auto it = waiters_.begin(); it != waiters_.end(); ++it) {
    if (timestamp < (*it)->timestamp) {
      break;
    }
    insert_pos = std::next(it);
  }
  waiters_.insert(insert_pos, waiter);
  
  // Wait for grant or abort signal
  waiter->cv->wait(lock, [&waiter]() {
    return waiter->granted.load() || waiter->should_abort.load();
  });
  
  // Remove from wait list
  waiters_.remove(waiter);
  
  if (waiter->should_abort.load()) {
    return LockStatus::DENIED;
  }
  
  return LockStatus::ACQUIRED;
}

LockStatus RecordLock::TryAcquire(txnid_t txn_id, LogicalTimestamp timestamp, LockMode mode) {
  return Acquire(txn_id, timestamp, mode, false);
}

bool RecordLock::Release(txnid_t txn_id) {
  std::lock_guard<std::mutex> lock(mutex_);
  
  // Find and remove from holders
  auto it = std::find_if(holders_.begin(), holders_.end(),
                         [txn_id](const LockHolder& h) { return h.txn_id == txn_id; });
  
  if (it == holders_.end()) {
    return false;  // Not held
  }
  
  holders_.erase(it);
  
  // Try to grant to waiters
  GrantWaiters();
  
  return true;
}

void RecordLock::GrantWaiters() {
  // Process waiters in order (already sorted by timestamp)
  auto it = waiters_.begin();
  while (it != waiters_.end()) {
    auto& waiter = *it;
    
    if (CanGrant(waiter->mode, waiter->txn_id)) {
      // Grant the lock
      holders_.emplace_back(waiter->txn_id, waiter->timestamp, waiter->mode);
      waiter->granted.store(true);
      waiter->cv->notify_one();
      it = waiters_.erase(it);
    } else {
      // Check wait-die for remaining waiters
      if (!ShouldWait(waiter->timestamp)) {
        // Waiter is now younger than holder - must abort
        waiter->should_abort.store(true);
        waiter->cv->notify_one();
        it = waiters_.erase(it);
      } else {
        ++it;
      }
    }
  }
}

bool RecordLock::IsHeldBy(txnid_t txn_id) const {
  std::lock_guard<std::mutex> lock(mutex_);
  
  for (const auto& holder : holders_) {
    if (holder.txn_id == txn_id) {
      return true;
    }
  }
  return false;
}

bool RecordLock::IsExclusivelyHeld() const {
  std::lock_guard<std::mutex> lock(mutex_);
  
  for (const auto& holder : holders_) {
    if (holder.mode == LockMode::EXCLUSIVE) {
      return true;
    }
  }
  return false;
}

LogicalTimestamp RecordLock::GetOldestHolderTimestamp() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return FindOldestHolder();
}

std::vector<LockHolder> RecordLock::GetHolders() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return holders_;
}

size_t RecordLock::GetWaiterCount() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return waiters_.size();
}

bool RecordLock::IsFree() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return holders_.empty();
}

// ============================================================================
// MoccLockManager Implementation
// ============================================================================

void MoccLockManager::RegisterTransaction(txnid_t txn_id, LogicalTimestamp timestamp) {
  std::lock_guard<std::mutex> lock(txn_mutex_);
  txn_states_.emplace(txn_id, TransactionLockState(txn_id, timestamp));
}

void MoccLockManager::UnregisterTransaction(txnid_t txn_id) {
  std::lock_guard<std::mutex> lock(txn_mutex_);
  txn_states_.erase(txn_id);
}

LogicalTimestamp MoccLockManager::GetTransactionTimestamp(txnid_t txn_id) const {
  std::lock_guard<std::mutex> lock(txn_mutex_);
  auto it = txn_states_.find(txn_id);
  if (it != txn_states_.end()) {
    return it->second.timestamp;
  }
  // Return a default timestamp if not registered
  return LogicalTimestamp();
}

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
                                        LockMode mode, bool blocking) {
  // Get transaction timestamp
  LogicalTimestamp timestamp = GetTransactionTimestamp(txn_id);
  if (!timestamp.IsValid()) {
    // Transaction not registered - use current clock
    timestamp = LogicalClockManager::Instance().GetTimestamp();
    RegisterTransaction(txn_id, timestamp);
  }
  
  // Check if we should use pessimistic locking
  if (!ShouldUsePessimisticLock(row, col_id)) {
    // Cold record - just return ACQUIRED (use OCC path)
    return LockStatus::ACQUIRED;
  }
  
  RecordLock& lock = GetOrCreateLock(row, col_id);
  LockStatus status = lock.Acquire(txn_id, timestamp, mode, blocking);
  
  if (status == LockStatus::ACQUIRED) {
    // Track this lock for the transaction
    std::lock_guard<std::mutex> guard(txn_mutex_);
    auto it = txn_states_.find(txn_id);
    if (it != txn_states_.end()) {
      LockInfo info(row, col_id, mode, timestamp);
      // Insert in sorted order for canonical mode
      auto& locks = it->second.held_locks;
      auto pos = std::lower_bound(locks.begin(), locks.end(), info);
      locks.insert(pos, info);
    }
  }
  
  return status;
}

LockStatus MoccLockManager::TryAcquireLock(txnid_t txn_id, void* row, int col_id, LockMode mode) {
  return AcquireLock(txn_id, row, col_id, mode, false);
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
    // Remove from transaction's held locks
    std::lock_guard<std::mutex> guard(txn_mutex_);
    auto tx_it = txn_states_.find(txn_id);
    if (tx_it != txn_states_.end()) {
      auto& locks = tx_it->second.held_locks;
      locks.erase(std::remove_if(locks.begin(), locks.end(),
                                 [row, col_id](const LockInfo& l) {
                                   return l.row == row && l.col_id == col_id;
                                 }),
                  locks.end());
    }
  }
  
  return released;
}

void MoccLockManager::ReleaseAllLocks(txnid_t txn_id) {
  std::vector<RowColKey> to_release;
  
  // Get all locks held by this transaction
  {
    std::lock_guard<std::mutex> guard(txn_mutex_);
    auto it = txn_states_.find(txn_id);
    if (it != txn_states_.end()) {
      for (const auto& lock_info : it->second.held_locks) {
        to_release.push_back(RowColKey{lock_info.row, lock_info.col_id});
      }
      it->second.held_locks.clear();
    }
  }
  
  // Release each lock
  std::shared_lock<std::shared_mutex> lock(mutex_);
  for (const auto& key : to_release) {
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
  std::lock_guard<std::mutex> guard(txn_mutex_);
  auto it = txn_states_.find(txn_id);
  if (it != txn_states_.end()) {
    return it->second.held_locks;  // Already sorted
  }
  return {};
}

bool MoccLockManager::PreAcquireLocks(txnid_t txn_id, const std::vector<LockInfo>& locks) {
  // Sort locks to prevent deadlock (already should be sorted, but ensure)
  std::vector<LockInfo> sorted_locks = locks;
  std::sort(sorted_locks.begin(), sorted_locks.end());
  
  std::vector<RowColKey> acquired;
  
  for (const auto& lock_info : sorted_locks) {
    LockStatus status = AcquireLock(txn_id, lock_info.row, lock_info.col_id, 
                                    lock_info.mode, true);  // blocking
    
    if (status != LockStatus::ACQUIRED) {
      // Failed to acquire - rollback all acquired locks
      for (const auto& key : acquired) {
        ReleaseLock(txn_id, key.row, key.col_id);
      }
      return false;
    }
    acquired.push_back(RowColKey{lock_info.row, lock_info.col_id});
  }
  
  return true;
}

void MoccLockManager::Clear() {
  std::unique_lock<std::shared_mutex> lock(mutex_);
  std::lock_guard<std::mutex> guard(txn_mutex_);
  
  locks_.clear();
  txn_states_.clear();
}

MoccLockManager::Stats MoccLockManager::GetStats() const {
  Stats stats{0, 0, 0, 0};
  
  std::shared_lock<std::shared_mutex> lock(mutex_);
  stats.total_locks = locks_.size();
  
  for (const auto& pair : locks_) {
    if (!pair.second->IsFree()) {
      stats.held_locks++;
    }
    stats.waiting_requests += pair.second->GetWaiterCount();
  }
  
  std::lock_guard<std::mutex> guard(txn_mutex_);
  stats.registered_txns = txn_states_.size();
  
  return stats;
}

} // namespace janus
