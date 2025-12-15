# MOCC Implementation in Mako

**Mostly-Optimistic Concurrency Control (MOCC)** is a hybrid concurrency control protocol that combines the benefits of optimistic and pessimistic locking. This implementation adds MOCC features to the Mako database system.

## Overview

MOCC addresses the limitations of pure OCC under high contention by:
1. **Learning from aborts** via a Retrospective Lock List (RLL)
2. **Tracking record "hotness"** to selectively use pessimistic locking
3. **Using Wait-Die** for deadlock prevention when acquiring locks

## Components

### 1. Lamport Clock (`lamport_clock.h`)
Provides globally unique, totally ordered timestamps for transactions.
- Format: `(counter << 16) | node_id`
- Used for Wait-Die ordering decisions

### 2. Temperature Tracking (`tuple.h`)
Each record tracks contention via a temperature counter:
- `heat_up()` - Called on conflict
- `cool_down()` - Called on successful commit
- `is_hot()` - Returns true if `temperature >= 128`

### 3. Wait-Die Protocol (`tuple.h`)
`try_lock_wait_die(write_intent, timestamp)`:
- **Older (lower timestamp)** transactions wait for younger lock holders
- **Younger (higher timestamp)** transactions abort immediately

### 4. Retrospective Lock List (`txn.h`, `txn_impl.h`)
- Tracks tuples that caused conflicts
- Preserved across retries via thread-local storage
- On retry: RLL tuples get pessimistic locks early

## Files Modified

| File | Changes |
|------|---------|
| `lamport_clock.h` | **NEW** - Lamport clock implementation |
| `tuple.h` | Temperature, lock_holder_timestamp, Wait-Die locking |
| `txn.h` | RLL, ordering_timestamp, thread-local storage |
| `txn.cc` | Thread-local definitions |
| `txn_impl.h` | RLL checks in `do_tuple_read`, Wait-Die in commit |
| `mako.hh` | Lamport clock initialization |
| `bench.cc` | Retry loop with RLL preservation |

## Configuration

| Parameter | Location | Default | Description |
|-----------|----------|---------|-------------|
| `TEMPERATURE_THRESHOLD` | `tuple.h` | 128 | Threshold for "hot" records |
| `MAX_SPINS` | `tuple.h` | 1000 | Wait-Die spin iterations |
| Retry enabled | `bench.cc` | Yes | Transaction retry on abort |

## Usage

Build and run benchmark:
```bash
make -j12
./examples/benchmark_mako.sh --threads 4 --duration 10
```

## Future Work

- [ ] RLL for write-set records (currently read-set only)
- [ ] Adaptive temperature threshold
- [ ] `std::unordered_set` for O(1) RLL lookup
- [ ] Wound-Wait as alternative to Wait-Die
- [ ] Cross-shard Lamport clock synchronization

## References

- Wang et al., "Mostly-Optimistic Concurrency Control for Highly Contended Dynamic Workloads on a Thousand Cores" (VLDB 2016)
