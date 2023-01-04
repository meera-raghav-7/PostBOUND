#!/bin/bash

# Experiment 03: Comparison of linear UES queries to subquery variants
# Requires: Postgres v14
# Requires: IMDB/JOB

ROOT=$(pwd)

cd $ROOT/postgres
echo "... Setting up Postgres environment"
. ./postgres-start.sh

echo "... Generating linear UES workload"
./ues-generator.py --pattern "*.sql" --generate-labels --out-col query --join-paths --timing \
    --table-estimation precise \
    --subqueries disabled \
    --out-col query --out workloads/job-ues-workload-base-linear.csv \
    $ROOT/workloads/JOB-Queries/implicit

echo "... Running linear UES workload"
./experiment-runner.py --csv --per-query-repetitions 3 --query-mod analyze --experiment-mode ues --out workloads/job-ues-results-base-linear.csv workloads/job-ues-workload-base-linear.csv

echo "... Gathering results"
# TODO: how/what should we do here exactly?

cd $ROOT/postgres
echo "... Cleaning up"
./postgres-stop.sh

cd $ROOT
echo "... Experiment done"
