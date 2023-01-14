#!/bin/bash

# Experiment 05: Runtime improvements via IdxNLJ hints in subqueries
# Requires: Postgres v14
# Requires: IMDB/JOB

ROOT=$(pwd)

cd $ROOT/postgres
echo "... Setting up Postgres environment"
. ./postgres-start.sh

cd $ROOT
echo "... Loading IMDB dataset"
util/setup-job.sh

cd $ROOT/ues
./set-workload.sh job
mkdir -p $ROOT/ues/workloads

echo "... Generating IdxNLJ hints for basic UES workload"
./query-hinting.py --mode ues-idxnlj --out workloads/job-ues-workload-idxnlj.csv workloads/job-ues-workload-base.csv

echo "... Running IdxNLJ workload"
./experiment-runner.py --csv --csv-col query --per-query-repetitions $QUERY_REPETITIONS \
        --experiment-mode ues --query-mod analyze \
        --hint-col hint \
        --out workloads/job-ues-results-idxnlj.csv \
        workloads/job-ues-workload-idxnlj.csv

cd $ROOT/postgres
echo "... Cleaning up"
. ./postgres-stop.sh

cd $ROOT
echo "... Experiment done"
