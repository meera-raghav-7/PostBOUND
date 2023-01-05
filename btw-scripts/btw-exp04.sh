#!/bin/bash

# Experiment 04: Bound improvements via different top-k lists
# Requires: Postgres v14
# Requires: IMDB/JOB

ROOT=$(pwd)

cd $ROOT/postgres
echo "... Setting up Postgres environment"
. ./postgres-start.sh --force

cd $ROOT/ues
echo "... Generating workloads for the cautious bound"
TOPKSETTINGS=(1 2 3 4 5)
for topk in ${TOPKSETTINGS[*]}; do
    ./ues-generator.py --pattern "*.sql" --timing --generate-labels --join-paths \
        --table-estimation precise \
        --join-estimation topk --topk-length $topk \
        --subqueries smart \
        --out-col query --out workloads/topk-setups/job-ues-workload-topk-$topk-smart.csv \
        ../workloads/JOB-Queries/implicit
done

echo "... Generating workloads for the approximate bound"
TOPKSETTINGS=(1 2 3 4 5 10 20 50 100 500)
for topk in ${TOPKSETTINGS[*]}; do
    ./ues-generator.py --pattern "*.sql" --timing --generate-labels --join-paths \
        --table-estimation precise \
        --join-estimation topk-approx --topk-length $topk \
        --subqueries smart \
        --out-col query --out workloads/topk-setups/job-ues-workload-topk-$topk-approx-smart.csv \
        ../workloads/JOB-Queries/implicit
done

echo "... Running workloads for the cautious bound"
TOPKSETTINGS=(1 2 3 4 5)
for topk in ${TOPKSETTINGS[*]}; do
    ./experiment-runner.py --csv --csv-col query --per-query-repetitions 3 \
        --experiment-mode ues --query-mod analyze \
        --out workloads/topk-setups/job-ues-results-topk-$topk-smart.csv \
        workloads/topk-setups/job-ues-workload-topk-$topk-smart.csv
done

echo "... Running workloads for the approximate bound"
TOPKSETTINGS=(1 2 3 4 5 10 20 50 100 500)
for topk in ${TOPKSETTINGS[*]}; do
    ./experiment-runner.py --csv --csv-col query --per-query-repetitions 3 \
        --experiment-mode ues --query-mod analyze \
        --out workloads/topk-setups/job-ues-results-topk-$topk-approx-smart.csv \
        workloads/topk-setups/job-ues-workload-topk-$topk-approx-smart.csv
done

cd $ROOT/postgres
echo "... Cleaning up"
./postgres-stop.sh

cd $ROOT
echo "... Experiment done"
