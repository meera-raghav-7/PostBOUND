# Reproducing the PostBOUND results @ BTW'23

The main entry point to reproduce our experimental results and to generate an adapted version of our paper is the
`run.sh` script.

This script does not require any additional arguments and can be executed as-is. It will do the following:

1. build a Docker image in which the actual experiments will be executed. The image is based on the `Dockerfile` that
was shipped with the `run.sh` script and this README.
2. generate a Docker container based on the image. This container will mount the `results` directory, which in the end
will contain all results produced by the experiments (see below)
3. start the actual experiments. This includes setting up a number of Postgres instances from scratch, creating
different databases and query workloads


## Requirements

It is recommended although not strictly necessary to run the Docker image on a system with at least **32 GB of RAM**.
The experiments do _not_ use any special hardware like GPUs, nor do they make use of advanced CPU features. Therefore,
the image should be executable on AMD-based systems as well as Intel-based architectures, although we only tested the
latter. Since the experiments include repeatetly setting up instances of the IMDB, at least **70 GB of storage** should
be available. Furthermore, a working **internet connection** is required, with broadband being heavily recommended.

In total, the experiments will take several hours, but the total runtime should not exceed days. On our hardware
(Intel Xeon 6126, 92GB main memory),
it took about 10 hours to execute the entire pipeline. To put the amount of work in the pipeline into perspective,
here is an overview of the most time-demanding steps in the pipeline:

- install a number of packages using `apt`
- install some R libraries and Python packages
- download an image of the IMDB
- download and compile two instances of PostgreSQL
- load IMDB instances from CSV files for a total of 4 times [^fn-imdb]
- execute the JOB for a total of 66 times using differently optimized queries [^fn-job]
- compile the final LaTeX paper

[^fn-imdb]: This ensures that settings using the native query optimizer encouter the same (fresh) DB state each time
they are run. Most importantly, this prevents Postgres from optimizing the $n$-th workload iteration based on metadata
it created during the $(n-1)$-th run.
[^fn-job]: there are 22 distinct settings and each setting is repeated 3 times to prevent some outliers


## Result artifacts

All results of the experiments are store in the `results` directory. This includes a freshly compiled version of our
paper (called `paper.pdf`), as well as an auxillary document called `evaluation_report.txt` with additional information.

The paper is generated and updated as follows:

- Figure 1 is rendered based on the specific UES workload generated during the experiments, although it only depends
on the calculated upper bounds which should be static. The corresponding experiment is experiment 1.
- Table 1 is updated based on the runtime measures of experiment 2
- Figure 8 is rendered based on the bounds and optimization times obtained in experiment 4 (sic.). Once again, the
upper bounds themselves should be static.

Notably, Figures 7 and 9 are not updated during optimization, because they were layouted manually. Instead, experiment
3 reproduces the results of Figure 7 and experiment 5 reproduces the results of Figure 9. To mitigate the missing
figures, the `evaluation_report.txt` contains a textual description of the results, equivalent to (but not as pretty
as) the figures.

Furthermore, all performance measurements that only appear in the text parts of the paper are left as-is. Still, the
raw data that percentages, etc. are based on, is exported in the `raw` subfolder. Lastly, the description of the
underlying hardware of the experiments is static as well.


## Repeating experiments

The easiest way to rerun experiments, is by deleting the Docker image and restarting the `run.sh` script. Keep in mind,
that this indeed repeats _all_ experiments and _all_ setup.

If only a specific subset of the experiments should be rerun, this can be achieved by restarting the corresponding
experiment scripts (see above) _from within the Docker container_. However, to ensure their successfull completion,
the system has to be setup according to the steps in `btw-start.sh`. Most importantly, this includes activating the
Python virtual environment. If in doubt, take a look at the commands in the `btw-start.sh` script. To export the results
and update the paper, etc., execute the `btw-tex.sh` script.


## Cleaning up

Since all experiments take place in the Docker container, it is sufficient to delete both the container, as well as the
underlying image to remove all artifacts. Afterwards, they can be re-created by running the `run.sh` script again.
