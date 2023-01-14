#!/bin/bash

ROOT=$(pwd)
mkdir -p $ROOT/results/raw

echo "... Preparing LaTex env"
cp $ROOT/table-01.tex $ROOT/tex-sources/
cp $ROOT/ues/evaluation/*.pdf $ROOT/tex-sources/figures/

cd $ROOT/tex-sources
echo "... Compiling LaTeX file"
latex -interaction=batchmode main.tex
bibtex main
latex -interaction=batchmode main.tex
latex -interaction=batchmode main.tex
pdflatex -interaction=batchmode main.tex

echo "... Exporting results"
cp $ROOT/evaluation_report.txt $ROOT/results/
cp $ROOT/tex-sources/main.pdf $ROOT/results/paper.pdf
cp $ROOT/ues/workloads/*.csv $ROOT/results/raw/
cp $ROOT/ues/workloads/topk-setups/*.csv $ROOT/results/raw

cd $ROOT
echo "... Done"
echo "... Available results are stored in $ROOT/results/"
