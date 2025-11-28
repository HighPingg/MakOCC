/**
 * MOCC Scheduler Implementation
 */

#include "scheduler.h"
#include "tx.h"
#include "../config.h"

namespace janus {

SchedulerMocc::SchedulerMocc() : SchedulerOcc() {
  // SchedulerOcc already sets up mdb_txn_mgr_ as TxnMgrOCC
  // MOCC uses the same underlying transaction manager
}

mdb::Txn* SchedulerMocc::get_mdb_txn(const i64 tid) {
  // Use parent implementation - MOCC uses OCC's underlying transaction
  return SchedulerOcc::get_mdb_txn(tid);
}

bool SchedulerMocc::DoPrepare(txnid_t tx_id) {
  // Use the extended version and just return success/fail
  MoccPrepareResult result = DoPrepareWithLockInfo(tx_id);
  return result.success;
}

MoccPrepareResult SchedulerMocc::DoPrepareWithLockInfo(txnid_t tx_id) {
  MoccPrepareResult result;
  
  auto sp_tx = dynamic_pointer_cast<TxMocc>(GetOrCreateTx(tx_id));
  if (!sp_tx) {
    // Fallback to regular OCC if not a MOCC transaction
    result.success = SchedulerOcc::DoPrepare(tx_id);
    return result;
  }
  
  auto txn = (mdb::TxnOCC*) get_mdb_txn(tx_id);
  verify(txn != nullptr);
  verify(txn->outcome_ == mdb::symbol_t::NONE);
  verify(!txn->verified_);
  
  // Apply temperature decay periodically
  ApplyTemperatureDecay();
  
  // Perform OCC validation (version check)
  if (sp_tx->is_leader_hint_ && !txn->version_check()) {
    Log_debug("MOCC: Version check failed for tx %" PRIx64, tx_id);
    
    // Collect conflict information for retry
    result.required_locks = DetermineRequiredLocks(tx_id);
    result.held_locks = sp_tx->GetAcquiredLocks();
    
    // Update temperature for conflicting records
    sp_tx->RecordAbortOnRecords();
    
    txn->__debug_abort_ = 1;
    result.success = false;
    return result;
  }
  
  // OCC lock acquisition phase (for validation)
  // For hot records, we should already have locks from execution phase
  for (auto& it : txn->ver_check_read_) {
    Row* row = it.first.row;
    auto* v_row = (mdb::VersionedRow*) row;
    
    Log_debug("MOCC: r_lock row: %llx", row);
    
    // Check if we already have a MOCC lock
    if (!MoccLockManager::Instance().HoldsLock(tx_id, row, it.first.col_id)) {
      // Try to acquire OCC read lock
      if (!v_row->rlock_row_by(txn->id())) {
        Log_debug("MOCC: Failed to acquire read lock for tx %" PRIx64, tx_id);
        
        // Release any locks we've acquired
        for (auto& lit : txn->locks_) {
          Row* r = lit.first;
          verify(r->rtti() == mdb::symbol_t::ROW_VERSIONED);
          auto vr = (mdb::VersionedRow*) r;
          vr->unlock_row_by(txn->id());
        }
        txn->locks_.clear();
        
        // Collect conflict info
        result.required_locks = DetermineRequiredLocks(tx_id);
        result.held_locks = sp_tx->GetAcquiredLocks();
        
        sp_tx->RecordAbortOnRecords();
        
        txn->__debug_abort_ = 1;
        result.success = false;
        return result;
      }
      insert_into_map(txn->locks_, row, -1);
    }
  }
  
  // Acquire write locks
  for (auto& it : txn->updates_) {
    Row* row = it.first;
    auto v_row = (mdb::VersionedRow*) row;
    
    Log_debug("MOCC: w_lock row: %llx", row);
    
    // Check if we already have a MOCC exclusive lock
    if (!MoccLockManager::Instance().HoldsLock(tx_id, row, -1)) {
      if (!v_row->wlock_row_by(txn->id())) {
        Log_debug("MOCC: Failed to acquire write lock for tx %" PRIx64, tx_id);
        
        // Release all locks
        for (auto& lit : txn->locks_) {
          Row* r = lit.first;
          verify(r->rtti() == mdb::symbol_t::ROW_VERSIONED);
          auto vr = (mdb::VersionedRow*) r;
          vr->unlock_row_by(txn->id());
        }
        txn->locks_.clear();
        
        result.required_locks = DetermineRequiredLocks(tx_id);
        result.held_locks = sp_tx->GetAcquiredLocks();
        
        sp_tx->RecordAbortOnRecords();
        
        txn->__debug_abort_ = 1;
        result.success = false;
        return result;
      }
      insert_into_map(txn->locks_, row, -1);
    }
  }
  
  Log_debug("MOCC: tx %" PRIx64 " validation succeeded", tx_id);
  txn->__debug_abort_ = 0;
  txn->verified_ = true;
  
  result.success = true;
  result.held_locks = sp_tx->GetAcquiredLocks();
  return result;
}

void SchedulerMocc::DoCommit(Tx& tx) {
  // Get MOCC transaction if available
  TxMocc* mocc_tx = dynamic_cast<TxMocc*>(&tx);
  
  if (mocc_tx) {
    // Update temperature on successful commit
    mocc_tx->RecordCommitOnRecords();
    
    // Release MOCC locks
    mocc_tx->ReleaseMoccLocks();
  }
  
  // Call parent commit
  SchedulerOcc::DoCommit(tx);
}

void SchedulerMocc::DoAbort(Tx& tx) {
  // Get MOCC transaction if available
  TxMocc* mocc_tx = dynamic_cast<TxMocc*>(&tx);
  
  if (mocc_tx) {
    // Update temperature on abort
    mocc_tx->RecordAbortOnRecords();
    
    // Release MOCC locks
    mocc_tx->ReleaseMoccLocks();
  }
  
  // Standard abort handling
  auto mdb_txn = RemoveMTxn(tx.tid_);
  verify(mdb_txn == tx.mdb_txn_);
  mdb_txn->abort();
  delete mdb_txn;
}

bool SchedulerMocc::PreAcquireLocksForTransaction(txnid_t tx_id,
                                                   const std::vector<LockInfo>& locks) {
  auto sp_tx = dynamic_pointer_cast<TxMocc>(GetOrCreateTx(tx_id));
  if (!sp_tx) {
    Log_warn("MOCC: PreAcquireLocks called on non-MOCC transaction");
    return false;
  }
  
  // Mark this as a retry with pre-acquired locks
  sp_tx->SetRetryWithLocks(true);
  
  // Try to acquire all specified locks
  return sp_tx->PreAcquireLocks(locks);
}

std::vector<LockInfo> SchedulerMocc::GetHeldLocks(txnid_t tx_id) const {
  return MoccLockManager::Instance().GetHeldLocks(tx_id);
}

void SchedulerMocc::ReleaseLocks(txnid_t tx_id) {
  MoccLockManager::Instance().ReleaseAllLocks(tx_id);
}

bool SchedulerMocc::IsHotTransaction(txnid_t tx_id) const {
  auto it = dtxns_.find(tx_id);
  if (it == dtxns_.end()) {
    return false;
  }
  
  TxMocc* mocc_tx = dynamic_cast<TxMocc*>(it->second.get());
  return mocc_tx && mocc_tx->IsHotTransaction();
}

void SchedulerMocc::ApplyTemperatureDecay() {
  TemperatureTracker::Instance().ApplyDecay();
}

TemperatureTracker::Stats SchedulerMocc::GetTemperatureStats() const {
  return TemperatureTracker::Instance().GetStats();
}

MoccLockManager::Stats SchedulerMocc::GetLockStats() const {
  return MoccLockManager::Instance().GetStats();
}

std::vector<LockInfo> SchedulerMocc::CollectConflictInfo(txnid_t tx_id) {
  std::vector<LockInfo> conflicts;
  
  auto sp_tx = dynamic_pointer_cast<TxMocc>(GetOrCreateTx(tx_id));
  if (sp_tx) {
    conflicts = sp_tx->GetConflictingRecords();
  }
  
  return conflicts;
}

std::vector<LockInfo> SchedulerMocc::DetermineRequiredLocks(txnid_t tx_id) {
  std::vector<LockInfo> required;
  
  auto sp_tx = dynamic_pointer_cast<TxMocc>(GetOrCreateTx(tx_id));
  if (!sp_tx) {
    return required;
  }
  
  // Get all accessed records that are hot or caused conflicts
  const auto& history = sp_tx->GetAccessHistory();
  
  for (const auto& access : history) {
    // Include all hot records
    TemperatureLevel level = TemperatureTracker::Instance().GetLevel(
      access.row, access.col_id);
    
    if (level == TemperatureLevel::HOT || level == TemperatureLevel::WARM) {
      LockMode mode = access.is_write ? LockMode::EXCLUSIVE : LockMode::SHARED;
      required.emplace_back(access.row, access.col_id, mode);
    }
  }
  
  // Also include any explicitly conflicting records
  auto conflicts = sp_tx->GetConflictingRecords();
  required.insert(required.end(), conflicts.begin(), conflicts.end());
  
  // Remove duplicates
  std::sort(required.begin(), required.end(),
            [](const LockInfo& a, const LockInfo& b) {
              if (a.row != b.row) return a.row < b.row;
              return a.col_id < b.col_id;
            });
  required.erase(std::unique(required.begin(), required.end(),
                             [](const LockInfo& a, const LockInfo& b) {
                               return a.row == b.row && a.col_id == b.col_id;
                             }),
                 required.end());
  
  return required;
}

} // namespace janus




