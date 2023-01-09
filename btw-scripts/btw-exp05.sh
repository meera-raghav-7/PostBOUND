#!/bin/bash

# Experiment 05: Bound improvements via different top-k lists
# Requires: Postgres v14
# Requires: IMDB/JOB

ROOT=$(pwd)

cd $ROOT/postgres
echo "... Setting up Postgres environment"
. ./postgres-start.sh

cd $ROOT/ues
echo "... Generating IdxNLJ hints for basic UES workload"
./set-workload.sh job
./query-hinting.py --mode ues-idxnlj --out workloads/job-ues-workload-idxnlj.csv workloads/job-ues-workload-base.csv

echo "... Running IdxNLJ workload"
./experiment-runner.py --csv --csv-col query --per-query-repetitions 3 \
        --experiment-mode ues --query-mod analyze \
        --hint-col hint
        --out workloads/topk-setups/job-ues-results-idxnlj.csv \
        workloads/job-ues-workload-idxnlj.csv

cd $ROOT/postgres
echo "... Cleaning up"
. ./postgres-stop.sh

cd $ROOT
echo "... Experiment done"
