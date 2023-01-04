#!/bin/bash

ORIG_ROOT=$(pwd)

if [ -n "$1" ]; then
    TARGET_DIR="$1"
else
    TARGET_DIR="BTW23-PostBOUND"
fi

echo ".. Setting up environment"
echo ".. Target directory is $TARGET_DIR"

git clone --recurse-submodules --branch btw23-reproducibility --depth 1 https://github.com/rbergm/PostBOUND.git $TARGET_DIR

echo "... Preparing directory structure"
cd $ORIG_ROOT/$TARGET_DIR
ROOT=$(pwd)
mv $ORIG_ROOT/btw-exp*.sh .
mv $ORIG_ROOT/btw-setup.R .

echo ".. Setting up Python venv"
python3 -m venv postbound-venv
. postbound-venv/bin/activate
pip install wheel
pip install -r requirements.txt

echo ".. Setting up R environment"
Rscript --vanilla btw-setup.R

cd $ROOT/postgres
echo ".. Setting up Postgres v14"
./postgres-setup.sh
./postgres-stop.sh
./postgres-psycopg-setup.sh job imdb
./postgres-psycopg-setup.sh stack stack_dummy
./postgres-psycopg-setup.sh tpch tpch
mv .psycopg_connection_* $ROOT/ues

cd $ROOT/postgres_12_4
echo ".. Setting up Postgres v12"
./postgres-setup.sh
./postgres-stop.sh

cd $ROOT
echo ".. Starting Experiment 01 :: Figure 01 - UES overestimation"
./btw-exp01.sh

cd $ROOT
echo ".. Starting Experiment 02 :: Table 01 - Benchmark runtimes"
./btw-exp02.sh

cd $ROOT
echo ".. Starting Experiment 03 :: Figure 07 - Subquery speedup"
./btw-exp03.sh

cd $ROOT
echo ".. Starting Experiment 04 :: Figure 08 - top-k influence"
./btw-exp04.sh

cd $ROOT
echo ".. Starting Experiment 05 :: Figure 09 - IdxNLJ operators"
./btw-exp05.sh

cd $ROOT/ues
echo ".. Generating result figures"
Rscript --vanilla evaluation/plots-presentation.R
python3 postbound-eval.py  # TODO

cd $ROOT
echo ".. Creating final paper"
# TODO

cd $ORIG_ROOT
echo ".. Done"
