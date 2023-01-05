#!/bin/bash

ROOT=$(pwd)

echo "... Preparing LaTex env"
cp $ROOT/BTW23-PostBOUND/table-01-tex $ROOT/tex-sources/
cp $ROOT/BTW23-PostBOUND/ues/evaluation/*.pdf $ROOT/tex-sources/figures/
cp $ROOT/BTW23-PostBOUND/evaluation_report.txt $ROOT/results/


cd $ROOT/tex-sources
echo "... Compiling LaTex file"
latex -interaction=batchmode main.tex
bibtex main
latex -interaction=batchmode main.tex
latex -interaction=batchmode main.tex
pdflatex -interaction=batchmode main.tex
cp main.pdf $ROOT/results/paper.pdf

cd $ROOT
echo "... Done"
echo "... Available results are stored in $ROOT/results/"
