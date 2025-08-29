#ifndef _MAKO_COMMON_H_
#define _MAKO_COMMON_H_

#include <fstream>
#include <sstream>
#include <vector>
#include <utility>
#include <string>
#include <set>

#include <getopt.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/sysinfo.h>

#include "masstree/config.h"

#include "allocator.h"
#include "stats_server.h"

#include "benchmarks/bench.h"
#include "benchmarks/sto/sync_util.hh"
#include "benchmarks/mbta_wrapper.hh"
#include "benchmarks/common.h"
#include "benchmarks/common2.h"
#include "benchmarks/benchmark_config.h"

#include "deptran/s_main.h"

#include "lib/configuration.h"
#include "lib/fasttransport.h"
#include "lib/common.h"
#include "lib/server.h"
#include "lib/rust_wrapper.h"


// Initialize Rust wrapper: communicate with rust-based redis client
// @safe
static void initialize_rust_wrapper()
{
  RustWrapper* g_rust_wrapper = new RustWrapper();
  if (!g_rust_wrapper->init()) {
      std::cerr << "Failed to initialize rust wrapper!" << std::endl;
      delete g_rust_wrapper;
      std::quick_exit( EXIT_SUCCESS );
  }
  std::cout << "Successfully initialized rust wrapper!" << std::endl;
  g_rust_wrapper->start_polling();
}

static void print_system_info()
{
  auto& benchConfig = BenchmarkConfig::getInstance();
  const unsigned long ncpus = coreid::num_cpus_online();
  cerr << "Database Benchmark:"                           << endl;
  cerr << "  pid: " << getpid()                           << endl;
  cerr << "settings:"                                     << endl;
  cerr << "  num-cpus    : " << ncpus                     << endl;
  cerr << "  num-threads : " << benchConfig.getNthreads()  << endl;
  cerr << "  shardIndex  : " << benchConfig.getShardIndex()<< endl;
  cerr << "  paxos_proc_name  : " << benchConfig.getPaxosProcName() << endl;
  cerr << "  nshards     : " << benchConfig.getNshards()   << endl;
  cerr << "  is_micro    : " << benchConfig.getIsMicro()   << endl;
  cerr << "  is_replicated : " << benchConfig.getIsReplicated()   << endl;
#ifdef USE_VARINT_ENCODING
  cerr << "  var-encode  : yes"                           << endl;
#else
  cerr << "  var-encode  : no"                            << endl;
#endif

#ifdef USE_JEMALLOC
  cerr << "  allocator   : jemalloc"                      << endl;
#elif defined USE_TCMALLOC
  cerr << "  allocator   : tcmalloc"                      << endl;
#elif defined USE_FLOW
  cerr << "  allocator   : flow"                          << endl;
#else
  cerr << "  allocator   : libc"                          << endl;
#endif
  cerr << "system properties:" << endl;

#ifdef TUPLE_PREFETCH
  cerr << "  tuple_prefetch          : yes" << endl;
#else
  cerr << "  tuple_prefetch          : no" << endl;
#endif

#ifdef BTREE_NODE_PREFETCH
  cerr << "  btree_node_prefetch     : yes" << endl;
#else
  cerr << "  btree_node_prefetch     : no" << endl;
#endif
}

// init all threads
static abstract_db* init() {
  auto& benchConfig = BenchmarkConfig::getInstance();

  initialize_rust_wrapper();

  // initialize the numa allocator
  size_t numa_memory = mako::parse_memory_spec("1G");
  if (numa_memory > 0) {
    const size_t maxpercpu = util::iceil(
        numa_memory / benchConfig.getNthreads(), ::allocator::GetHugepageSize());
    numa_memory = maxpercpu * benchConfig.getNthreads();
    ::allocator::Initialize(benchConfig.getNthreads(), maxpercpu);
  }

  // Print system information
  print_system_info();

  sync_util::sync_logger::Init(benchConfig.getShardIndex(), benchConfig.getNshards(), 
                               benchConfig.getNthreads(), 
                               benchConfig.getLeaderConfig()==1, /* is leader */ 
                               benchConfig.getCluster(),
                               benchConfig.getConfig());
  
  abstract_db *db = new mbta_wrapper; // on the leader replica
  return db; 
}

#endif