#ifndef _NDB_BENCHMARK_CONFIG_H_
#define _NDB_BENCHMARK_CONFIG_H_

#include <stdint.h>
#include <string>
#include "lib/configuration.h"

enum {
  RUNMODE_TIME = 0,
  RUNMODE_OPS  = 1
};

class BenchmarkConfig {
  private:
      // Private constructor with default values
      BenchmarkConfig() : 
          nthreads_(1),
          num_erpc_server_(2), // number of erpc pull threads
          scale_factor_(1.0),
          nshards_(1),
          shardIndex_(0),
          cluster_("localhost"),
          clusterRole_(0), 
          workload_type_(1), // 0: simpleShards (debug); 1: tpcc/microbenchmark
          config_(nullptr),
          running_(true),
          control_mode_(0),
          verbose_(1),
          txn_flags_(1),
          runtime_(30),
          runtime_plus_(0),
          ops_per_worker_(0),
          run_mode_(RUNMODE_TIME),
          enable_parallel_loading_(0),
          pin_cpus_(1),
          slow_exit_(0),
          retry_aborted_transaction_(1),
          no_reset_counters_(0),
          backoff_aborted_transaction_(0),
          use_hashtable_(0),
          is_micro_(0) {} // if run micro-based workload
      
      // Member variables
      size_t nthreads_;
      size_t nshards_;
      size_t num_erpc_server_;
      size_t shardIndex_;
      std::string cluster_;
      int clusterRole_;
      size_t workload_type_;
      transport::Configuration* config_;
      volatile bool running_;
      volatile int control_mode_;
      int verbose_;
      uint64_t txn_flags_;
      double scale_factor_;
      uint64_t runtime_;
      volatile int runtime_plus_;
      uint64_t ops_per_worker_;
      int run_mode_;
      int enable_parallel_loading_;
      int pin_cpus_;
      int slow_exit_;
      int retry_aborted_transaction_;
      int no_reset_counters_;
      int backoff_aborted_transaction_;
      int use_hashtable_;
      int is_micro_;
      int is_replicated_;

  public:
      // Delete copy/move constructors
      BenchmarkConfig(const BenchmarkConfig&) = delete;
      BenchmarkConfig& operator=(const BenchmarkConfig&) = delete;

      // Single point of access
      static BenchmarkConfig& getInstance() {
          static BenchmarkConfig instance;
          return instance;
      }

      // Getters
      size_t getNthreads() const { return nthreads_; }
      size_t getNshards() const { return nshards_; }
      size_t getNumErpcServer() const { return num_erpc_server_; }
      size_t getShardIndex() const { return shardIndex_; }
      const std::string& getCluster() const { return cluster_; }
      int getClusterRole() const { return clusterRole_; }
      size_t getWorkloadType() const { return workload_type_; }
      transport::Configuration* getConfig() const { return config_; }
      bool isRunning() const { return running_; }
      int getControlMode() const { return control_mode_; }
      int getVerbose() const { return verbose_; }
      uint64_t getTxnFlags() const { return txn_flags_; }
      double getScaleFactor() const { return scale_factor_; }
      uint64_t getRuntime() const { return runtime_; }
      int getRuntimePlus() const { return runtime_plus_; }
      uint64_t getOpsPerWorker() const { return ops_per_worker_; }
      int getRunMode() const { return run_mode_; }
      int getEnableParallelLoading() const { return enable_parallel_loading_; }
      int getPinCpus() const { return pin_cpus_; }
      int getSlowExit() const { return slow_exit_; }
      int getRetryAbortedTransaction() const { return retry_aborted_transaction_; }
      int getNoResetCounters() const { return no_reset_counters_; }
      int getBackoffAbortedTransaction() const { return backoff_aborted_transaction_; }
      int getUseHashtable() const { return use_hashtable_; }
      int getIsMicro() const { return is_micro_; }
      int getIsReplicated() const { return is_replicated_; }

      // Setters
      void setNthreads(size_t n) { nthreads_ = n; }
      void setNshards(size_t n) { nshards_ = n; }
      void setNumErpcServer(size_t n) { num_erpc_server_ = n; }
      void setShardIndex(size_t idx) { shardIndex_ = idx; }
      void setCluster(const std::string& c) { cluster_ = c; }
      void setClusterRole(int role) { clusterRole_ = role; }
      void setWorkloadType(size_t type) { workload_type_ = type; }
      void setConfig(transport::Configuration* cfg) { config_ = cfg; }
      void setRunning(bool r) { running_ = r; }
      void setControlMode(int mode) { control_mode_ = mode; }
      void setVerbose(int v) { verbose_ = v; }
      void setTxnFlags(uint64_t flags) { txn_flags_ = flags; }
      void setScaleFactor(double sf) { scale_factor_ = sf; }
      void setRuntime(uint64_t rt) { runtime_ = rt; }
      void setRuntimePlus(int rtp) { runtime_plus_ = rtp; }
      void setOpsPerWorker(uint64_t ops) { ops_per_worker_ = ops; }
      void setRunMode(int mode) { run_mode_ = mode; }
      void setEnableParallelLoading(int enable) { enable_parallel_loading_ = enable; }
      void setPinCpus(int pin) { pin_cpus_ = pin; }
      void setSlowExit(int slow) { slow_exit_ = slow; }
      void setRetryAbortedTransaction(int retry) { retry_aborted_transaction_ = retry; }
      void setNoResetCounters(int no_reset) { no_reset_counters_ = no_reset; }
      void setBackoffAbortedTransaction(int backoff) { backoff_aborted_transaction_ = backoff; }
      void setUseHashtable(int use) { use_hashtable_ = use; }
      void setIsMicro(int micro) { is_micro_ = micro; }
      void setIsReplicated(int replicated) { is_replicated_ = replicated; }
};

#endif /* _NDB_BENCHMARK_CONFIG_H_ */