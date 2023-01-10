#!/bin/bash

# Experiment 03: Comparison of linear UES queries to subquery variants
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

echo "... Generating UES workload with subqueries"
./ues-generator.py --pattern "*.sql" --generate-labels --out-col query --join-paths --timing \
    --table-estimation precise \
    --join-estimation topk-approx --topk-length 20 \
    --subqueries smart \
    --out-col query --out workloads/topk-setups/job-ues-workload-topk-20-approx-smart.csv \
    $ROOT/workloads/JOB-Queries/implicit

echo "... Generating linear UES workload"
./ues-generator.py --pattern "*.sql" --generate-labels --out-col query --join-paths --timing \
    --table-estimation precise \
    --join-estimation topk-approx --topk-length 20 \
    --subqueries disabled \
    --out-col query --out workloads/topk-setups/job-ues-workload-topk-20-approx-linear.csv \
    $ROOT/workloads/JOB-Queries/implicit

echo "... Running UES workload with subqueries"
./experiment-runner.py --csv --per-query-repetitions 3 \
    --query-mod analyze --experiment-mode ues \
    --out workloads/topk-setups/job-ues-results-topk-20-approx-smart.csv \
    workloads/topk-setups/job-ues-workload-topk-20-approx-smart.csv

echo "... Running linear UES workload"
./experiment-runner.py --csv --per-query-repetitions 3 \
    --query-mod analyze --experiment-mode ues \
    --out workloads/topk-setups/job-ues-results-topk-20-approx-linear.csv \
    workloads/topk-setups/job-ues-workload-topk-20-approx-linear.csv

cd $ROOT/postgres
echo "... Cleaning up"
. ./postgres-stop.sh

cd $ROOT
echo "... Experiment done"
