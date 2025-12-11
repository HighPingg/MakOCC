/**
 * MOCC Scheduler Implementation
 * 
 * DoPrepare follows MOCC Algorithm 1 commit() function:
 * 1. SHARED locks for hot reads (prevent clobbered reads)
 * 2. EXCLUSIVE locks for hot writes
 * 3. OCC validation for cold records
 * 
 * Key requirements met:
 * - Hot records: pessimistic locking
 * - Cold records: optimistic validation
 * - ALWAYS release all locks on commit/abort
 */

#include "scheduler.h"
#include "tx.h"
#include "../config.h"

namespace janus {

SchedulerMocc::SchedulerMocc() : SchedulerOcc() {
  // SchedulerOcc already sets up mdb_txn_mgr_ as TxnMgrOCC
}

mdb::Txn* SchedulerMocc::get_mdb_txn(const i64 tid) {
  return SchedulerOcc::get_mdb_txn(tid);
}

void SchedulerMocc::InitializeTransaction(txnid_t tx_id, LogicalTimestamp timestamp) {
  auto sp_tx = dynamic_pointer_cast<TxMocc>(GetOrCreateTx(tx_id));
  if (sp_tx) {
    // Set up transaction timestamp
    // Note: InitializeTimestamp gets a new timestamp from the clock
    // Here we're setting it from coordinator's timestamp
    MoccLockManager::Instance().RegisterTransaction(tx_id, timestamp);
  }
}

bool SchedulerMocc::DoPrepare(txnid_t tx_id) {
  MoccPrepareResult result = DoPrepareWithResult(tx_id);
  return result.success;
}

MoccPrepareResult SchedulerMocc::DoPrepareWithResult(txnid_t tx_id) {
  MoccPrepareResult result;
  
  auto sp_tx = dynamic_pointer_cast<TxMocc>(GetOrCreateTx(tx_id));
  auto txn = (mdb::TxnOCC*) get_mdb_txn(tx_id);
  
  verify(txn != nullptr);
  verify(txn->outcome_ == mdb::symbol_t::NONE);
  verify(!txn->verified_);
  
  // Apply temperature decay periodically
  ApplyTemperatureDecay();
  
  // ============================================================================
  // STEP 1: Acquire SHARED locks for hot records in READ set
  // (MOCC paper: prevents clobbered reads)
  // ============================================================================
  if (sp_tx && sp_tx->is_leader_hint_) {
    if (!AcquireReadLocksForHotRecords(tx_id, txn)) {
      Log_debug("MOCC: Failed to acquire read locks for hot records, tx: %" PRIx64, tx_id);
      HandleAbort(tx_id);
      result.success = false;
      return result;
    }
  }
  
  // ============================================================================
  // STEP 2: Acquire EXCLUSIVE locks for hot records in WRITE set
  // ============================================================================
  if (sp_tx && sp_tx->is_leader_hint_) {
    if (!AcquireWriteLocksForHotRecords(tx_id, txn)) {
      Log_debug("MOCC: Failed to acquire write locks for hot records, tx: %" PRIx64, tx_id);
      HandleAbort(tx_id);
      result.success = false;
      return result;
    }
  }
  
  // ============================================================================
  // STEP 3: OCC validation for cold records (version check + lock acquisition)
  // ============================================================================
  if (sp_tx && sp_tx->is_leader_hint_) {
    if (!ValidateColdRecords(tx_id, txn)) {
      Log_debug("MOCC: Cold record validation failed, tx: %" PRIx64, tx_id);
      HandleAbort(tx_id);
      result.success = false;
      return result;
    }
  }
  
  Log_debug("MOCC: tx %" PRIx64 " validation succeeded", tx_id);
  txn->verified_ = true;
  result.success = true;
  
  return result;
}

bool SchedulerMocc::AcquireReadLocksForHotRecords(txnid_t tx_id, mdb::TxnOCC* txn) {
  auto& lock_mgr = MoccLockManager::Instance();
  
  for (auto& it : txn->ver_check_read_) {
    Row* row = it.first.row;
    int col_id = it.first.col_id;
    
    // Check if this record is HOT
    if (!IsHot(row, col_id)) {
      continue;  // Cold record - skip, will be validated via OCC
    }
    
    // Hot record - need SHARED lock
    Log_debug("MOCC: Acquiring shared lock for hot read, row: %p, col: %d, tx: %" PRIx64,
              row, col_id, tx_id);
    
    // Check if we already have the lock
    if (lock_mgr.HoldsLock(tx_id, row, col_id)) {
      continue;  // Already have lock
    }
    
    // Acquire lock with wait-die (blocking mode for canonical order)
    LockStatus status = lock_mgr.AcquireLock(tx_id, row, col_id, 
                                              LockMode::SHARED, true);
    
    if (status != LockStatus::ACQUIRED) {
      Log_debug("MOCC: Failed to acquire shared lock (wait-die abort), tx: %" PRIx64, tx_id);
      return false;  // Wait-Die: must abort
    }
  }
  
  return true;
}

bool SchedulerMocc::AcquireWriteLocksForHotRecords(txnid_t tx_id, mdb::TxnOCC* txn) {
  auto& lock_mgr = MoccLockManager::Instance();
  
  for (auto& it : txn->updates_) {
    Row* row = it.first;
    int col_id = -1;  // Row-level lock for writes
    
    // Check if this record is HOT
    if (!IsHot(row, col_id)) {
      continue;  // Cold record - skip, will be locked via OCC
    }
    
    // Hot record - need EXCLUSIVE lock
    Log_debug("MOCC: Acquiring exclusive lock for hot write, row: %p, tx: %" PRIx64,
              row, tx_id);
    
    // Check if we already have the lock
    if (lock_mgr.HoldsLock(tx_id, row, col_id)) {
      continue;  // Already have lock (might need upgrade though)
    }
    
    // Acquire lock with wait-die (blocking mode for canonical order)
    LockStatus status = lock_mgr.AcquireLock(tx_id, row, col_id,
                                              LockMode::EXCLUSIVE, true);
    
    if (status != LockStatus::ACQUIRED) {
      Log_debug("MOCC: Failed to acquire exclusive lock (wait-die abort), tx: %" PRIx64, tx_id);
      return false;  // Wait-Die: must abort
    }
  }
  
  return true;
}

bool SchedulerMocc::ValidateColdRecords(txnid_t tx_id, mdb::TxnOCC* txn) {
  auto& lock_mgr = MoccLockManager::Instance();
  
  // First, perform OCC version check for all records
  // This is done by TxnOCC::version_check() internally
  if (!txn->version_check()) {
    Log_debug("MOCC: OCC version check failed for tx: %" PRIx64, tx_id);
    return false;
  }
  
  // Acquire read locks for cold records
  for (auto& it : txn->ver_check_read_) {
    Row* row = it.first.row;
    int col_id = it.first.col_id;
    
    // Skip hot records (already have MOCC locks)
    if (IsHot(row, col_id)) {
      continue;
    }
    
    // Cold record - acquire OCC read lock
    auto* v_row = (mdb::VersionedRow*) row;
    if (!v_row->rlock_row_by(txn->id())) {
      Log_debug("MOCC: Failed to acquire OCC read lock, tx: %" PRIx64, tx_id);
      return false;
    }
    insert_into_map(txn->locks_, row, -1);
  }
  
  // Acquire write locks for cold writes
  for (auto& it : txn->updates_) {
    Row* row = it.first;
    
    // Skip hot records (already have MOCC locks)
    if (IsHot(row, -1)) {
      continue;
    }
    
    // Cold record - acquire OCC write lock
    auto* v_row = (mdb::VersionedRow*) row;
    if (!v_row->wlock_row_by(txn->id())) {
      Log_debug("MOCC: Failed to acquire OCC write lock for cold record, tx: %" PRIx64, tx_id);
      
      // Release locks acquired so far
      for (auto& lit : txn->locks_) {
        Row* r = lit.first;
        auto vr = (mdb::VersionedRow*) r;
        vr->unlock_row_by(txn->id());
      }
      txn->locks_.clear();
      
      return false;
    }
    insert_into_map(txn->locks_, row, -1);
  }
  
  return true;
}

void SchedulerMocc::DoCommit(Tx& tx) {
  Log_debug("MOCC: DoCommit for tx %" PRIx64, tx.tid_);
  
  TxMocc* mocc_tx = dynamic_cast<TxMocc*>(&tx);
  auto txn = (mdb::TxnOCC*) tx.mdb_txn_;
  
  // ============================================================================
  // MUST RELEASE ALL LOCKS
  // ============================================================================
  
  // 1. Release MOCC locks
  if (mocc_tx) {
    mocc_tx->ReleaseMoccLocks();
  }
  
  // 2. Release OCC locks
  if (txn) {
    ReleaseOccLocks(txn);
  }
  
  // 3. Call parent commit (applies writes)
  SchedulerOcc::DoCommit(tx);
  
  // Note: Temperature NOT decreased on commit (MOCC paper)
}

void SchedulerMocc::DoAbort(Tx& tx) {
  Log_debug("MOCC: DoAbort for tx %" PRIx64, tx.tid_);
  
  TxMocc* mocc_tx = dynamic_cast<TxMocc*>(&tx);
  auto txn = (mdb::TxnOCC*) tx.mdb_txn_;
  
  // ============================================================================
  // Update temperature for ALL accessed records (MOCC paper)
  // ============================================================================
  if (mocc_tx) {
    mocc_tx->RecordAbortOnRecords();
  }
  
  // ============================================================================
  // MUST RELEASE ALL LOCKS
  // ============================================================================
  
  // 1. Release MOCC locks
  if (mocc_tx) {
    mocc_tx->ReleaseMoccLocks();
  }
  
  // 2. Release OCC locks
  if (txn) {
    ReleaseOccLocks(txn);
  }
  
  // 3. Standard abort handling
  auto mdb_txn = RemoveMTxn(tx.tid_);
  verify(mdb_txn == tx.mdb_txn_);
  mdb_txn->abort();
  delete mdb_txn;
}

void SchedulerMocc::HandleAbort(txnid_t tx_id) {
  auto sp_tx = dynamic_pointer_cast<TxMocc>(GetOrCreateTx(tx_id));
  
  // Update temperature on all accessed records
  UpdateTemperatureOnAbort(tx_id);
  
  // Release all MOCC locks
  MoccLockManager::Instance().ReleaseAllLocks(tx_id);
  
  // Release OCC locks
  auto txn = (mdb::TxnOCC*) get_mdb_txn(tx_id);
  if (txn) {
    ReleaseOccLocks(txn);
  }
  
  Log_debug("MOCC: HandleAbort completed for tx %" PRIx64, tx_id);
}

void SchedulerMocc::ReleaseOccLocks(mdb::TxnOCC* txn) {
  for (auto& it : txn->locks_) {
    Row* row = it.first;
    verify(row->rtti() == mdb::symbol_t::ROW_VERSIONED);
    auto v_row = (mdb::VersionedRow*) row;
    v_row->unlock_row_by(txn->id());
  }
  txn->locks_.clear();
}

void SchedulerMocc::UpdateTemperatureOnAbort(txnid_t tx_id) {
  auto sp_tx = dynamic_pointer_cast<TxMocc>(GetOrCreateTx(tx_id));
  
  if (sp_tx) {
    // Use transaction's access history
    sp_tx->RecordAbortOnRecords();
  } else {
    // Fallback: update temperature via OCC transaction
    auto txn = (mdb::TxnOCC*) get_mdb_txn(tx_id);
    if (txn) {
      auto& temp_tracker = TemperatureTracker::Instance();
      
      for (auto& it : txn->ver_check_read_) {
        temp_tracker.RecordAbort(it.first.row, it.first.col_id);
      }
      
      for (auto& it : txn->updates_) {
        temp_tracker.RecordAbort(it.first, -1);
      }
    }
  }
}

void SchedulerMocc::ReleaseLocks(txnid_t tx_id) {
  // Release all MOCC locks
  MoccLockManager::Instance().ReleaseAllLocks(tx_id);
  
  // Release OCC locks
  auto txn = (mdb::TxnOCC*) get_mdb_txn(tx_id);
  if (txn) {
    ReleaseOccLocks(txn);
  }
}

bool SchedulerMocc::IsHot(void* row, int col_id) const {
  TemperatureLevel level = TemperatureTracker::Instance().GetLevel(row, col_id);
  return level == TemperatureLevel::HOT;
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

} // namespace janus

