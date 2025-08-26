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


#endif