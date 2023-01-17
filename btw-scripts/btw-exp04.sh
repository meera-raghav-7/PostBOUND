#!/bin/bash

# Experiment 04: Bound improvements via different top-k lists
# Requires: Postgres v14
# Requires: IMDB/JOB

ROOT=$(pwd)
APPROX_TOPK_SETTINGS=(1 2 3 4 5 10 20 50 100 500)
CAUTIOUS_TOPK_SETTINGS=(1 2 3 4 5)


cd $ROOT/postgres
echo "... Setting up Postgres environment"
. ./postgres-start.sh

cd $ROOT
echo "... Loading IMDB dataset"
util/setup-job.sh

cd $ROOT/ues
./set-workload.sh job
mkdir -p $ROOT/ues/workloads/topk-setups

# In addition to the smart subqueries for the top-k approx workloads, we also need the linear variants. This is due to
# a weird Python-bug that causes the optimization time to increase substantially when generating many subqueries
# (as is the case with large top-k settings and smart generation strategy). More specifically, the bug seems to be
# caused by our implementation of a join tree and how it handles the insertion of subqueries (which should be a cheap
# process). Therefore, we compare the optimization time of the linear workload in this case. For our pipeline this
# means that we need to generate and execute a number of linear workloads in addition to the bushy counterparts that
# we normally focus our evaluation on, in order to ensure that our analysis still works correctly.

echo ".. Generating base workloads to compare to"
./ues-generator.py --pattern "*.sql" --timing --generate-labels --join-paths \
        --table-estimation precise \
        --join-estimation basic \
        --subqueries smart \
        --out-col query --out workloads/job-ues-workload-base-smart.csv \
        ../workloads/JOB-Queries/implicit
./ues-generator.py --pattern "*.sql" --timing --generate-labels --join-paths \
        --table-estimation precise \
        --join-estimation basic \
        --subqueries disabled \
        --out-col query --out workloads/job-ues-workload-base-linear.csv \
        ../workloads/JOB-Queries/implicit

echo ".. Running base workloads"
./experiment-runner.py --csv --csv-col query --per-query-repetitions $QUERY_REPETITIONS \
        --experiment-mode ues --query-mod analyze \
        --out workloads/job-ues-results-base-smart.csv \
        workloads/job-ues-workload-base-smart.csv
./experiment-runner.py --csv --csv-col query --per-query-repetitions $QUERY_REPETITIONS \
        --experiment-mode ues --query-mod analyze \
        --out workloads/job-ues-results-base-linear.csv \
        workloads/job-ues-workload-base-linear.csv

echo "... Generating workloads for the cautious bound"
for topk in ${CAUTIOUS_TOPK_SETTINGS[*]}; do
    ./ues-generator.py --pattern "*.sql" --timing --generate-labels --join-paths \
        --table-estimation precise \
        --join-estimation topk --topk-length $topk \
        --subqueries smart \
        --out-col query --out workloads/topk-setups/job-ues-workload-topk-$topk-smart.csv \
        ../workloads/JOB-Queries/implicit
done

echo "... Generating workloads for the approximate bound"
for topk in ${APPROX_TOPK_SETTINGS[*]}; do
    ./ues-generator.py --pattern "*.sql" --timing --generate-labels --join-paths \
        --table-estimation precise \
        --join-estimation topk-approx --topk-length $topk \
        --subqueries smart \
        --out-col query --out workloads/topk-setups/job-ues-workload-topk-$topk-approx-smart.csv \
        ../workloads/JOB-Queries/implicit

    ./ues-generator.py --pattern "*.sql" --timing --generate-labels --join-paths \
        --table-estimation precise \
        --join-estimation topk-approx --topk-length $topk \
        --subqueries disabled \
        --out-col query --out workloads/topk-setups/job-ues-workload-topk-$topk-approx-linear.csv \
        ../workloads/JOB-Queries/implicit
done

echo "... Running workloads for the cautious bound"
for topk in ${CAUTIOUS_TOPK_SETTINGS[*]}; do
    ./experiment-runner.py --csv --csv-col query --per-query-repetitions $QUERY_REPETITIONS \
        --experiment-mode ues --query-mod analyze \
        --out workloads/topk-setups/job-ues-results-topk-$topk-smart.csv \
        workloads/topk-setups/job-ues-workload-topk-$topk-smart.csv
done

echo "... Running workloads for the approximate bound"
for topk in ${APPROX_TOPK_SETTINGS[*]}; do
    ./experiment-runner.py --csv --csv-col query --per-query-repetitions $QUERY_REPETITIONS \
        --experiment-mode ues --query-mod analyze \
        --out workloads/topk-setups/job-ues-results-topk-$topk-approx-smart.csv \
        workloads/topk-setups/job-ues-workload-topk-$topk-approx-smart.csv

    ./experiment-runner.py --csv --csv-col query --per-query-repetitions $QUERY_REPETITIONS \
        --experiment-mode ues --query-mod analyze \
        --out workloads/topk-setups/job-ues-results-topk-$topk-approx-linear.csv \
        workloads/topk-setups/job-ues-workload-topk-$topk-approx-linear.csv
done

cd $ROOT/postgres
echo "... Cleaning up"
. ./postgres-stop.sh

cd $ROOT
echo "... Experiment done"
