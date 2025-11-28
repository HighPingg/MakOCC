# MOCC (Mixed Optimistic Concurrency Control) Implementation Changelog

## Overview

This document describes all changes made to implement MOCC in the Mako database system. MOCC is a hybrid concurrency control protocol that combines optimistic and pessimistic locking based on record "temperature" (contention level).

### Key Concept

MOCC tracks how frequently records are accessed and cause conflicts:
- **Cold records** (low contention): Use standard OCC - speculative execution with validation at commit
- **Hot records** (high contention): Use pessimistic locking - acquire locks before access

This hybrid approach reduces aborts for hot records while maintaining OCC's performance for cold records.

---

## Files Added

### 1. Temperature Tracking System

#### `src/deptran/mocc/temperature.h`
**Purpose**: Header file defining the temperature tracking mechanism.

**Key Components**:
- `TemperatureLevel` enum: COLD, WARM, HOT classifications
- `RecordTemperature` struct: Per-record temperature state (atomic for thread-safety)
- `TemperatureTracker` singleton: Global temperature management

**Design Decisions**:
- Used atomics for lock-free temperature updates in high-concurrency scenarios
- Temperature is tracked per (row, column) pair for fine-grained control
- Implemented exponential decay to prevent records from staying hot forever

**Constants**:
```cpp
MOCC_COLD_THRESHOLD = 3      // Below this = use OCC
MOCC_HOT_THRESHOLD = 10      // Above this = use locks
MOCC_MAX_TEMPERATURE = 20    // Cap to prevent unbounded growth
MOCC_DECAY_INTERVAL_MS = 100 // Decay check interval
MOCC_DECAY_FACTOR = 0.9      // Exponential decay rate
```

#### `src/deptran/mocc/temperature.cc`
**Purpose**: Implementation of temperature tracking.

**Key Methods**:
- `GetLevel()`: Returns COLD/WARM/HOT based on current temperature
- `IncrementTemperature()`: Called on every record access
- `RecordAbort()`: Increases temperature by 3 on conflict (faster hot detection)
- `RecordCommit()`: Decreases temperature by 1 on success (encourages optimism)
- `ApplyDecay()`: Periodic exponential decay to cool down records

---

### 2. MOCC Locking System

#### `src/deptran/mocc/mocc_lock.h`
**Purpose**: Header file defining MOCC's hybrid locking mechanism.

**Key Components**:
- `LockMode` enum: NONE, SHARED, EXCLUSIVE
- `LockStatus` enum: ACQUIRED, WAITING, DENIED, TIMEOUT
- `RecordLock` class: Per-record reader-writer lock with wait queue
- `MoccLockManager` singleton: Global lock management
- `LockInfo` struct: Lock descriptor for communication

**Design Decisions**:
- Used reader-writer locks to allow concurrent reads on hot records
- Implemented wait queue for fairness (FIFO ordering)
- Lock sorting in `PreAcquireLocks()` prevents deadlock during retry

#### `src/deptran/mocc/mocc_lock.cc`
**Purpose**: Implementation of MOCC locking.

**Key Methods**:
- `AcquireLock()`: Acquires lock (pessimistic for hot, skip for cold)
- `TryAcquireLock()`: Non-blocking lock attempt
- `ReleaseAllLocks()`: Transaction cleanup
- `PreAcquireLocks()`: Bulk lock acquisition for retry (sorted order)
- `ShouldUsePessimisticLock()`: Checks temperature to decide locking strategy

---

### 3. MOCC Transaction

#### `src/deptran/mocc/tx.h`
**Purpose**: Header file for MOCC transaction class.

**Key Extensions over TxOcc**:
- `access_history_`: Tracks all accesses for abort communication
- `is_retry_with_locks_`: Flag for pessimistic retry mode
- `pre_acquired_locks_`: Locks acquired before dispatch on retry

**Design Decisions**:
- Inherits from `TxOcc` to reuse OCC validation logic
- Tracks "hot access count" to identify hot transactions

#### `src/deptran/mocc/tx.cc`
**Purpose**: Implementation of MOCC transaction.

**Key Methods**:
- `ReadColumn()`: Temperature-aware read (lock if hot)
- `WriteColumn()`: Temperature-aware write (lock if hot)
- `PreAcquireLocks()`: Acquire locks before retry
- `RecordAbortOnRecords()`: Update temperatures on abort
- `RecordCommitOnRecords()`: Update temperatures on commit

---

### 4. MOCC Scheduler

#### `src/deptran/mocc/scheduler.h`
**Purpose**: Header file for MOCC scheduler.

**Key Extensions over SchedulerOcc**:
- `MoccPrepareResult`: Extended prepare result with lock information
- `DoPrepareWithLockInfo()`: Validation that returns required locks on failure

#### `src/deptran/mocc/scheduler.cc`
**Purpose**: Implementation of MOCC scheduler.

**Key Methods**:
- `DoPrepare()`: Standard prepare (delegates to `DoPrepareWithLockInfo`)
- `DoPrepareWithLockInfo()`: Validation with lock requirement collection
- `DoCommit()`: Commits and updates temperatures
- `DoAbort()`: Aborts and updates temperatures
- `PreAcquireLocksForTransaction()`: Pre-lock for retry
- `DetermineRequiredLocks()`: Analyzes access history to find hot records

**Key Change: Abort Handling**:
On validation failure, the scheduler now:
1. Collects all accessed hot/warm records
2. Returns them as `required_locks` in the result
3. Coordinator uses this for pessimistic retry

---

### 5. MOCC Coordinator

#### `src/deptran/mocc/coordinator.h`
**Purpose**: Header file for MOCC coordinator.

**Key Extensions over CoordinatorClassic**:
- `MoccRetryInfo`: Tracks required locks and retry count
- `abort_has_lock_info_`: Whether abort included lock requirements

#### `src/deptran/mocc/coordinator.cc`
**Purpose**: Implementation of MOCC coordinator.

**Key Methods**:
- `Restart()`: On retry, pre-acquires locks for hot records
- `PrepareAckMocc()`: Handles prepare response with lock info
- `CollectRequiredLocks()`: Aggregates locks from shard responses
- `PreAcquireLocksOnShards()`: Broadcasts lock requests before retry
- `ShouldUseLocksForRetry()`: Decides if retry should use locks

**Retry Strategy**:
1. First retry: Use OCC (record may have cooled)
2. Second+ retry with lock info: Use pessimistic locking
3. Hot transaction detection: If touches many hot records, use locks

---

### 6. MOCC Frame

#### `src/deptran/mocc/frame.h` & `src/deptran/mocc/frame.cc`
**Purpose**: Protocol registration with the system.

**What it does**:
- Registers `mocc`/`MOCC` as a valid protocol name
- Creates MOCC-specific coordinator, scheduler, and transaction objects

---

## Files Modified

### 1. `src/deptran/constants.h`
**Change**: Added `MODE_MOCC` constant (0x05)

```cpp
#define MODE_MOCC   (0x05)   // Mixed OCC - temperature-based hybrid locking
```

### 2. `src/deptran/frame.cc`
**Changes**:
- Added includes for MOCC components
- Added `MODE_MOCC` case in `GetFrame()`, `CreateCoordinator()`, `CreateTx()`, `CreateScheduler()`, `CreateRow()`
- Registered "mocc" and "MOCC" in `FrameNameToMode()` map

---

## Test Files Added

### `examples/test_mocc.cc`
**Purpose**: Comprehensive unit tests for MOCC.

**Test Categories**:

1. **Temperature Tests**:
   - `test_temperature_basic`: Initial state is cold
   - `test_temperature_increment`: Temperature increases on access
   - `test_temperature_hot`: Record becomes hot after many accesses
   - `test_temperature_abort_increases`: Abort increases temp by 3
   - `test_temperature_commit_decreases`: Commit decreases temp by 1
   - `test_temperature_max_cap`: Temperature capped at max
   - `test_temperature_stats`: Statistics collection

2. **Lock Tests**:
   - `test_lock_basic_acquire_release`: Basic lock lifecycle
   - `test_lock_shared_concurrent`: Multiple shared locks allowed
   - `test_lock_exclusive_blocks_shared`: Exclusive blocks shared
   - `test_lock_pre_acquire_multiple`: Bulk lock acquisition
   - `test_lock_stats`: Lock statistics

3. **Integration Tests**:
   - `test_mocc_cold_record_uses_occ`: Cold records skip locking
   - `test_mocc_hot_record_uses_locks`: Hot records use locking
   - `test_mocc_abort_makes_record_hotter`: Aborts increase temperature

4. **Concurrency Tests**:
   - `test_concurrent_temperature_updates`: Thread-safe temperature
   - `test_concurrent_lock_acquisition`: Concurrent exclusive locks

### `examples/test_mocc.sh`
**Purpose**: Shell script to run MOCC tests.

---

## Build System Changes

### `CMakeLists.txt`
**Changes**:
- Added `test_mocc` executable via `add_apps()`
- Added `test_mocc` to CTest test suite

---

## Algorithm Summary

### MOCC Read Operation
```
1. Check temperature of (row, col)
2. If HOT:
   a. Acquire SHARED lock
   b. If lock failed, return false
3. Record access in history
4. Increment temperature
5. Perform actual read (via parent OCC)
```

### MOCC Write Operation
```
1. Check temperature of (row, col)
2. If HOT:
   a. Acquire EXCLUSIVE lock
   b. If lock failed, return false
3. Record access in history
4. Increment temperature (by 2 for writes)
5. Perform actual write (via parent OCC)
```

### MOCC Validation (DoPrepare)
```
1. Apply temperature decay
2. Run OCC version check
3. If version check fails:
   a. Collect all hot/warm records accessed
   b. Update temperatures (RecordAbort)
   c. Return failure with required_locks
4. Acquire OCC validation locks
5. Return success
```

### MOCC Retry Strategy
```
1. On abort, coordinator receives required_locks
2. Increment retry_count
3. If retry_count >= 2 or has_lock_info:
   a. Pre-acquire all required_locks
   b. Mark transaction as retry_with_locks
4. Re-dispatch transaction
5. MOCC transaction uses pessimistic locking for all accesses
```

---

## Configuration

To use MOCC, set the protocol mode to `mocc` in your configuration:

```yaml
mode: mocc
```

Or in code:
```cpp
Config::GetConfig()->tx_proto_ = MODE_MOCC;
```

---

## Performance Characteristics

| Scenario | MOCC Behavior | Expected Outcome |
|----------|---------------|------------------|
| Low contention | Uses OCC | Same as OCC |
| High contention on few records | Pessimistic on hot records | Fewer aborts |
| Uniform high contention | Eventually all pessimistic | Similar to 2PL |
| Bursty contention | Adapts via temperature | Best of both |

---

## Future Work

1. **Distributed Temperature Sync**: Currently temperature is per-node; could sync across replicas
2. **Adaptive Thresholds**: Auto-tune COLD/HOT thresholds based on workload
3. **Lock Timeout**: Add timeout to blocking lock acquisition
4. **Batched Lock Communication**: Optimize RPC for lock broadcasting




