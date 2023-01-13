#!/bin/bash

# Experiment 02: benchmark results for different datasets, workloads and Postgres versions
# Requires: Postgres v14, Postgres v12
#Requires: IMDB/JOB, TPC-H/SSB

ROOT=$(pwd)


# ======================
# === JOB
# ======================

cd $ROOT/postgres
echo "... Setting up Postgres v14 environment"
. ./postgres-start.sh

cd $ROOT
echo "... Loading IMDB dataset"
util/setup-job.sh

cd $ROOT/ues
./set-workload.sh job
mkdir -p $ROOT/ues/workloads

echo "... Generating native query workload for JOB"
$ROOT/util/generate-workload.py --pattern "*.sql" --generate-labels \
    --out workloads/job-workload-implicit.csv \
    $ROOT/workloads/JOB-Queries/implicit

echo "... Running native workload for JOB, PG v14"
./experiment-runner.py --csv --per-query-repetitions $QUERY_REPETITIONS \
    --query-mod analyze --pg-param "SET enable_nestloop = 'off';" --pg-param "SET enable_memoize = 'off';" \
    --out workloads/job-results-implicit.csv \
    workloads/job-workload-implicit.csv

echo "... Generating UES queries for JOB"
./ues-generator.py --pattern "*.sql" --generate-labels \
    --join-paths --timing \
    --table-estimation precise \
    --subqueries smart
    --out-col query --out workloads/job-ues-workload-base.csv \
    $ROOT/workloads/JOB-Queries/implicit

echo "... Running UES workload for JOB, PG v14"
./experiment-runner.py --csv --per-query-repetitions $QUERY_REPETITIONS \
    --query-mod analyze --experiment-mode ues \
    --out workloads/job-ues-results-base.csv \
    workloads/job-ues-workload-base.csv

cd $ROOT/postgres
echo "... Cleaning up Postgres v14 environment"
if [ "$RESET_DATABASES" = "true" ] ; then
    dropdb imdb
fi
. ./postgres-stop.sh

cd $ROOT/postgres_12_4
echo "... Setting up Postgres v12 environment"
. ./postgres-start.sh

cd $ROOT
echo "... Loading IMDB dataset"
util/setup-job.sh

cd $ROOT/ues
echo "... Running native workload for JOB, PG v12"
./experiment-runner.py --csv --per-query-repetitions $QUERY_REPETITIONS \
    --query-mod analyze --pg-param "SET enable_nestloop = 'off';" \
    --out workloads/job-results-implicit-pg12_4.csv \
    workloads/job-workload-implicit.csv

echo "... Running UES workload for JOB, PG v12"
./experiment-runner.py --csv --per-query-repetitions $QUERY_REPETITIONS \
    --query-mod analyze --pg-param "SET enable_nestloop = 'off';" --pg-param "SET join_collapse_limit = 1;" \
    --out workloads/job-ues-results-base-pg12_4.csv \
    workloads/job-ues-workload-base.csv

cd $ROOT/postgres_12_4
echo "... Cleaning up Postgres v12 environment"
if [ "$RESET_DATABASES" = "true" ] ; then
    dropdb imdb
fi
. ./postgres-stop.sh


# ======================
# === SSB
# ======================

# At this point we assume that both PG versions have been set up already, so all that is left for us is to load the actual databases
cd $ROOT/postgres
echo "... Setting up Postgres v14 environment"
. ./postgres-start.sh

cd $ROOT/util
echo "... Loading SSB dataset"
./setup-ssb.sh 1

cd $ROOT/ues
./set-workload.sh ssb

echo "... Generating native query workload for SSB"
$ROOT/util/generate-workload.py --pattern "*.sql" --generate-labels \
    --out workloads/ssb-workload-implicit.csv \
    $ROOT/workloads/SSB-Queries

echo "... Running native workload for SSB, PG v14"
./experiment-runner.py --csv --per-query-repetitions $QUERY_REPETITIONS \
    --query-mod analyze --pg-param "SET enable_nestloop = 'off';" --pg-param "SET enable_memoize = 'off';" \
    --out workloads/ssb-results-implicit.csv \
    workloads/ssb-workload-implicit.csv

echo "... Generating UES queries for SSB"
./ues-generator.py --pattern "*.sql" --generate-labels \
    --join-paths --timing \
    --out-col query --out workloads/ssb-ues-workload-base.csv \
    $ROOT/workloads/SSB-Queries

echo "... Running UES workload for SSB, PG v14"
./experiment-runner.py --csv --per-query-repetitions $QUERY_REPETITIONS \
    --query-mod analyze --experiment-mode ues \
    --out workloads/ssb-ues-results-base.csv \
    workloads/ssb-ues-workload-base.csv

cd $ROOT/postgres
echo "... Cleaning up Postgres v14 environment"
if [ "$RESET_DATABASES" = "true" ] ; then
    dropdb tpch
fi
. ./postgres-stop.sh

cd $ROOT/postgres_12_4
echo "... Setting up Postgres v12 environment"
. ./postgres-start.sh

cd $ROOT/util
echo "... Loading SSB dataset"
./setup-ssb.sh 1

cd $ROOT/ues
echo "... Running native workload for SSB, PG v12"
./experiment-runner.py --csv --per-query-repetitions $QUERY_REPETITIONS \
    --query-mod analyze --pg-param "SET enable_nestloop = 'off';" \
    --out workloads/ssb-results-implicit-pg12_4.csv \
    workloads/ssb-workload-implicit.csv

echo "... Running UES workload for JOB, PG v12"
./experiment-runner.py --csv --per-query-repetitions $QUERY_REPETITIONS \
    --query-mod analyze --pg-param "SET enable_nestloop = 'off';" --pg-param "SET join_collapse_limit = 1;" \
    --out workloads/ssb-ues-results-base-pg12_4.csv \
    workloads/ssb-ues-workload-base.csv

cd $ROOT/postgres_12_4
echo "... Cleaning up Postgres v12 environment"
if [ "$RESET_DATABASES" = "true" ] ; then
    dropdb tpch
fi
. ./postgres-stop.sh

cd $ROOT
echo "... Experiment done"
