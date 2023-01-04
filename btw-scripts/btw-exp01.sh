#!/bin/bash

# Experiment 01: UES overestimation
# Requires: Postgres v14
# Requires: IMDB/JOB

ROOT=$(pwd)

cd $ROOT/postgres
echo "... Setting up Postgres v14 environment"
. ./postgres-start.sh

cd $ROOT
echo "... Loading IMDB dataset"
util/setup-job.sh

cd $ROOT/ues
echo "... Generating UES queries"
./set-workload.sh job
./ues-generator.py --pattern "*.sql" --generate-labels --out workloads/job-ues-workload-base.csv ../workloads/JOB-Queries/implicit
# the true cardinalities to compare to are static by nature. They are shipped in a separate CSV file in the Git repository

cd $ROOT
echo "... Cleaning up"
dropdb imdb
postgres/postgres-stop.sh

cd $ROOT
echo "... Experiment done"
