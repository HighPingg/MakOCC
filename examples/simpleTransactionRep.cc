//
// Simple Transaction Tests for Mako Database
//

#include <iostream>
#include <chrono>
#include <thread>
#include <vector>
#include <mako.hh>
#include "examples/common.h"
#include "benchmarks/rpc_setup.h"

using namespace std;
using namespace mako;

class TransactionWorker {
public:
    TransactionWorker(abstract_db *db) : db(db) {
        txn_obj_buf.reserve(str_arena::MinStrReserveLength);
        txn_obj_buf.resize(db->sizeof_txn_object(0));
    }

    void initialize() {
        scoped_db_thread_ctx ctx(db, false);
        mbta_ordered_index::mbta_type::thread_init();
    }

    void test_basic_transactions() {
        printf("\n--- Testing Basic Transactions ---\n");
        static abstract_ordered_index *table = db->open_index("customer_0");
        std::this_thread::sleep_for(std::chrono::seconds(1));

        // Write 5 keys
        for (size_t i = 0; i < 5; i++) {
            void *txn = db->new_txn(0, arena, txn_buf());
            std::string key = "test_key_" + std::to_string(i);
            std::string value = "test_value_" + std::to_string(i) + 
                               std::string(mako::EXTRA_BITS_FOR_VALUE, 'B');
            try {
                table->put(txn, key, StringWrapper(value));
                db->commit_txn(txn);
            } catch (abstract_db::abstract_abort_exception &ex) {
                printf("Write aborted: %s\n", key.c_str());
                db->abort_txn(txn);
            }
        }
        VERIFY_PASS("Write 5 records");

        // Read and verify 5 keys
        bool all_reads_ok = true;
        for (size_t i = 0; i < 5; i++) {
            void *txn = db->new_txn(0, arena, txn_buf());
            std::string key = "test_key_" + std::to_string(i);
            std::string value = "";
            try {
                table->get(txn, key, value);
                db->commit_txn(txn);
                
                std::string expected = "test_value_" + std::to_string(i);
                if (value.substr(0, expected.length()) != expected) {
                    all_reads_ok = false;
                    break;
                }
            } catch (abstract_db::abstract_abort_exception &ex) {
                printf("Read aborted: %s\n", key.c_str());
                db->abort_txn(txn);
                all_reads_ok = false;
                break;
            }
        }
        VERIFY(all_reads_ok, "Read and verify 5 records");

        // Scan and verify table
        auto scan_results = scan_tables(db, table);
        bool scan_ok = true;
        for (int i = 0; i < 5; i++) {
            std::string expected_key = "test_key_" + std::to_string(i);
            std::string expected_value = "test_value_" + std::to_string(i);
            
            if (scan_results[i].first != expected_key ||
                scan_results[i].second.substr(0, expected_value.length()) != expected_value) {
                scan_ok = false;
                break;
            }
        }
        VERIFY(scan_ok, "Table scan verification");
    }

protected:
    abstract_db *const db;
    str_arena arena;
    std::string txn_obj_buf;
    inline void *txn_buf() { return (void *)txn_obj_buf.data(); }
};

void run_worker_tests(abstract_db *db) {
    auto worker = new TransactionWorker(db);
    worker->initialize();
    worker->test_basic_transactions();
    delete worker;
}

void run_tests(abstract_db *db) {
    // tpcc.cc general logics
    // initialize: 1. thread init; 2. erpc threads; 3. helper threads
    
    // start different db worker threads - enforced
    size_t nshards = BenchmarkConfig::getInstance().getNshards();
    std::vector<std::thread> worker_threads;
    
    // Create a worker thread for each shard
    for (size_t i = 0; i < nshards; ++i) {
        worker_threads.emplace_back(run_worker_tests, db);
    }
    
    // Wait for all worker threads to complete
    for (auto& t : worker_threads) {
        t.join();
    }
}

int main(int argc, char **argv) {
    
    // All necessary parameters expected from users
    if (argc != 6) {
        printf("Usage: %s <nshards> <shardIdx> <nthreads> <paxos_proc_name> <is_replicated>\n", argv[0]);
        printf("Example: %s 2 0 6 localhost 1\n", argv[0]);
        return 1;
    }

    int nshards = std::stoi(argv[1]);
    int shardIdx = std::stoi(argv[2]);
    int nthreads = std::stoi(argv[3]);
    std::string paxos_proc_name = std::string(argv[4]);
    int is_replicated = std::stoi(argv[5]);

    // Build config path - fix the format string to use std::to_string
    std::string config_path = get_current_absolute_path() 
            + "../src/mako/config/local-shards" + std::to_string(nshards) 
            + "-warehouses" + std::to_string(nthreads) + ".yml";
    vector<string> paxos_config_file{
        get_current_absolute_path() + "../config/1leader_2followers/paxos" + std::to_string(nthreads) + "_shardidx" + std::to_string(shardIdx) + ".yml",
        get_current_absolute_path() + "../config/occ_paxos.yml"
    };
    
    auto& benchConfig = BenchmarkConfig::getInstance();
    benchConfig.setNshards(nshards);
    benchConfig.setShardIndex(shardIdx);
    benchConfig.setNthreads(nthreads);
    benchConfig.setPaxosProcName(paxos_proc_name);
    benchConfig.setIsReplicated(is_replicated);

    auto config = new transport::Configuration(config_path);
    benchConfig.setConfig(config);
    benchConfig.setPaxosConfigFile(paxos_config_file);

    // This variable is accessible until program ends as follower replays uses it
    TSharedThreadPoolMbta replicated_db (benchConfig.getNthreads()+1);
    init_env(replicated_db) ;

    printf("=== Mako Transaction Tests  ===\n");
    
    abstract_db * db = initWithDB();
    if (benchConfig.getLeaderConfig()) {
        run_tests(db);
    }

    db_close() ;
    
    printf("\n" GREEN "All tests completed successfully!" RESET "\n");
    return 0;
}