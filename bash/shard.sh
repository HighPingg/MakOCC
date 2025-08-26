#!/bin/bash
#sudo cgdelete -g cpuset:/cpulimit 2>/dev/null || true
#sudo cgcreate -t $USER:$USER -a $USER:$USER -g cpuset:/cpulimit
set -xv
nshard=$1
shard=$2
trd=$3
cluster=$4
is_micro=$5
is_replicated=$6
let up=trd+3
#sudo cgset -r cpuset.mems=0 cpulimit
#sudo cgset -r cpuset.cpus=0-$up cpulimit
mkdir -p results
path=$(pwd)/src/mako

# Build the command with optional flags
CMD="./build/dbtest --num-threads $trd --shard-index $shard --shard-config $path/config/local-shards$nshard-warehouses$trd.yml -F config/1leader_2followers/paxos$trd\_shardidx$shard.yml -F config/occ_paxos.yml -P $cluster"

# Add --is-micro flag if enabled (value is 1)
if [ "$is_micro" == "1" ]; then
    CMD="$CMD --is-micro"
fi

# Add --is-replicated flag if enabled (value is 1)
if [ "$is_replicated" == "1" ]; then
    CMD="$CMD --is-replicated"
fi

# sudo gdb --args 
# sudo strace -f -c
# sudo gdb --args cgexec -g cpuset:cpulimit
eval $CMD 
