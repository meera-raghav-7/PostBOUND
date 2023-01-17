#!/usr/bin/env python3

import json
import warnings
from typing import Any

import numpy as np
import pandas as pd

from analysis import selection
from postgres import explain
from transform import mosp

warnings.simplefilter("ignore")  # stops the 'Unknown Node' warnings during plan analysis for Gather, Merge, etc.


class Report:
    def __init__(self, outfile: str):
        self._outfile = outfile
        self._entries = []

    def next_section(self, title: str) -> None:
        if self._entries:
            self._entries.append({"type": "separator"})
            self._entries.append({"type": "separator"})
        if not title.endswith("\n"):
            title += "\n"
        self._entries.append({"type": "title", "value": title})

    def add_entry(self, value: str) -> None:
        if not value.endswith("\n"):
            value += "\n"
        self._entries.append({"type": "entry", "value": value})

    def add_separator(self) -> None:
        self._entries.append({"type": "separator"})

    def write(self) -> None:
        lines = []
        for entry in self._entries:
            if entry["type"] == "separator":
                lines.append("\n")
            elif entry["type"] == "title":
                text = entry["value"]
                lines.append(f"=== {text}")
            elif entry["type"] == "entry":
                lines.append(entry["value"])
            else:
                raise ValueError("Unknown entry type: " + entry["type"])

        with open(self._outfile, mode="w") as output:
            output.writelines(lines)


def read_workload_cautious(topk_length: int = np.nan, raw: str = "") -> pd.DataFrame:
    path = f"workloads/topk-setups/job-ues-results-topk-{topk_length}-smart.csv" if not raw else raw
    df = pd.read_csv(path)
    df = selection.best_query_repetition(df, "label", performance_col="query_rt_total")
    df = selection.reorder(df)
    df.set_index("label", inplace=True)
    df["query"] = df["query"].apply(mosp.MospQuery.parse)
    df["ues_bounds"] = df["ues_bounds"].apply(json.loads)
    df["query_result"] = df["query_result"].apply(json.loads)
    df["ues_final_bound"] = df["ues_final_bound"].astype("float")

    df.rename(columns={"query_rt_total": "execution_time", "ues_final_bound": "upper_bound"}, inplace=True)
    if not raw:
        df["mode"] = "top-k"
        df["topk_length"] = topk_length
    else:
        df["mode"] = "ues"
        df["topk_length"] = 0
    return df


def read_workload_approx(topk_length: int = np.nan, raw: str = "", linear: bool = False) -> pd.DataFrame:
    subquery_suffix = "linear" if linear else "smart"
    path = f"workloads/topk-setups/job-ues-results-topk-{topk_length}-approx-{subquery_suffix}.csv" if not raw else raw
    df = pd.read_csv(path)
    df = selection.best_query_repetition(df, "label", performance_col="query_rt_total")
    df = selection.reorder(df)
    df.set_index("label", inplace=True)
    df["query"] = df["query"].apply(mosp.MospQuery.parse)
    df["ues_bounds"] = df["ues_bounds"].apply(json.loads)
    df["query_result"] = df["query_result"].apply(json.loads)
    df["ues_final_bound"] = df["ues_final_bound"].astype("float")

    df.rename(columns={"query_rt_total": "execution_time", "ues_final_bound": "upper_bound"}, inplace=True)
    if not raw:
        df["mode"] = "top-k"
        df["topk_length"] = topk_length
    else:
        df["mode"] = "ues"
        df["topk_length"] = 0
    df["subquery_mode"] = subquery_suffix
    return df


def aggregate_cautious():
    job_cards = pd.read_csv("workloads/job-results-true-cards.csv", usecols=["label", "query_result"]).rename(columns={"query_result": "true_card"})
    all_workloads = [read_workload_cautious(raw="workloads/job-ues-results-base-smart.csv")]
    for topk_setting in range(1, 6):
        all_workloads.append(read_workload_cautious(topk_setting))
    results = pd.concat(all_workloads).reset_index().merge(job_cards, on="label")
    results["overestimation"] = (results["upper_bound"] + 1) / (results["true_card"] + 1)
    results["n_subqueries"] = results["query"].apply(lambda q: len(q.subqueries()))
    results["setting"] = np.where(results["mode"] == "ues", "UES", "Top-" + results["topk_length"].astype(str))
    settings = pd.Categorical(results["setting"], categories=["UES"] + list(map(lambda k: f"Top-{k}", sorted(filter(lambda k: k > 0, results.topk_length.unique())))), ordered=True)
    results["setting"] = settings
    results.drop(columns=["optimization_success", "ues_bounds", "query_result", "run"], inplace=True)
    return results


def aggregate_approx():
    job_cards = pd.read_csv("workloads/job-results-true-cards.csv", usecols=["label", "query_result"]).rename(columns={"query_result": "true_card"})
    all_workloads_approx = [read_workload_approx(raw="workloads/job-ues-results-base-smart.csv")]
    for topk_setting in [1, 5, 10, 20, 50, 100, 500]:
        all_workloads_approx.append(read_workload_approx(topk_setting))
    results_approx = pd.concat(all_workloads_approx).reset_index().merge(job_cards, on="label")
    results_approx["overestimation"] = (results_approx["upper_bound"] + 1) / (results_approx["true_card"] + 1)
    results_approx["n_subqueries"] = results_approx["query"].apply(lambda q: len(q.subqueries()))
    results_approx["setting"] = np.where(results_approx["mode"] == "ues", "UES", "Top-" + results_approx["topk_length"].astype(str))
    settings_approx = pd.Categorical(results_approx["setting"], categories=["UES"] + list(map(lambda k: f"Top-{k}", sorted(filter(lambda k: k > 0, results_approx.topk_length.unique())))), ordered=True)
    results_approx["setting"] = settings_approx
    results_approx.drop(columns=["optimization_success", "ues_bounds", "query_result", "run"], inplace=True)
    return results_approx


def write_tex_table(job_runtimes, ssb_runtimes, outfile):
    template = r"""
    \begin{table}[t]
        \centering
        \begin{tabular}{|c||c|c||c|c|}
            \hline
            Benchmark& \multicolumn{2}{c||}{PostgreSQL v12.4} & \multicolumn{2}{c|}{PostgreSQL v14.2} \\
            & Native & UES & Native & UES \\
            \hline
            Join-Order-Benchmark (JOB) & ###JOB_RT### \\
            Star-Schema-Bechmark (SSB) & ###SSB_RT### \\
            \hline
        \end{tabular}
        \caption{Total runtimes of different benchmarks using different PostgreSQL versions.}
        \label{tab:postbound-benchmarks}
        \vspace{-0.4cm}
    \end{table}
    """
    job_replaced = template.replace("###JOB_RT###", " & ".join(f"{round(rt, ndigits=1)}s" for rt in job_runtimes))
    ssb_replaced = job_replaced.replace("###SSB_RT###", " & ".join(f"{round(rt, ndigits=1)}s" for rt in ssb_runtimes))
    with open(outfile, "w") as output:
        output.write(ssb_replaced)


def read_workload(path: str, workload: str, optimization: str, pg_ver: Any) -> pd.DataFrame:
    df = pd.read_csv(path)
    df = selection.best_query_repetition(df, group_cols="label", performance_col="query_rt_total")
    df["workload"] = workload
    df["optimizer"] = optimization
    df["postgres_version"] = str(pg_ver)
    df.rename(columns={"query_rt_total": "execution_time"}, inplace=True)
    return df


def eval_01_join_orders(report: Report):
    job_ues_pg14 = read_workload("workloads/job-ues-results-base.csv", "JOB", "UES", 14.2)
    job_ues_pg12 = read_workload("workloads/job-ues-results-base-pg12_4.csv", "JOB", "UES", 12.4)
    job_nat_pg14 = read_workload("workloads/job-results-implicit.csv", "JOB", "native", 14.2)
    job_nat_pg12 = read_workload("workloads/job-results-implicit-pg12_4.csv", "JOB", "native", 12.4)

    ssb_ues_pg14 = read_workload("workloads/ssb-ues-results-base.csv", "SSB", "UES", 14.2)
    ssb_ues_pg12 = read_workload("workloads/ssb-ues-results-base-pg12_4.csv", "SSB", "UES", 12.4)
    ssb_nat_pg14 = read_workload("workloads/ssb-results-implicit.csv", "SSB", "native", 14.2)
    ssb_nat_pg12 = read_workload("workloads/ssb-results-implicit-pg12_4.csv", "SSB", "native", 12.4)

    df = pd.concat([job_ues_pg14, job_ues_pg12, job_nat_pg14, job_nat_pg12, ssb_ues_pg14,
                    ssb_ues_pg12, ssb_nat_pg14, ssb_nat_pg12])
    aggregated_runtimes = df.groupby(["workload", "optimizer", "postgres_version"]).execution_time.sum()

    report.next_section("Results for Table 01 - Total runtimes of different benchmarks using "
                        "different PostgreSQL versions")
    report.add_entry(str(aggregated_runtimes))

    job_runtimes = [aggregated_runtimes["JOB"]["native"]["12.4"], aggregated_runtimes["JOB"]["UES"]["12.4"],
                    aggregated_runtimes["JOB"]["native"]["14.2"], aggregated_runtimes["JOB"]["UES"]["14.2"]]
    ssb_runtimes = [aggregated_runtimes["SSB"]["native"]["12.4"], aggregated_runtimes["SSB"]["UES"]["12.4"],
                    aggregated_runtimes["SSB"]["native"]["14.2"], aggregated_runtimes["SSB"]["UES"]["14.2"]]
    write_tex_table(job_runtimes, ssb_runtimes, "../table-01.tex")


def eval_02_subquery_generation(report: Report):
    df_top20_lin = selection.best_query_repetition(
        pd.read_csv("workloads/topk-setups/job-ues-results-topk-20-approx-linear.csv"),
        group_cols="label", performance_col="query_rt_total")
    df_top20_sq = selection.best_query_repetition(
        pd.read_csv("workloads/topk-setups/job-ues-results-topk-20-approx-smart.csv"),
        group_cols="label", performance_col="query_rt_total")

    df_top20_lin.rename(columns={"query_result": "explain", "query_rt_total": "execution_time"}, inplace=True)
    df_top20_sq.rename(columns={"query_result": "explain", "query_rt_total": "execution_time",
                                "ues_final_bound": "upper_bound"}, inplace=True)
    df_top20_lin["query"] = df_top20_lin["query"].apply(mosp.MospQuery.parse)
    df_top20_sq["query"] = df_top20_sq["query"].apply(mosp.MospQuery.parse)
    df_top20_lin["explain"] = df_top20_lin["explain"].apply(json.loads)
    df_top20_sq["explain"] = df_top20_sq["explain"].apply(json.loads)
    df_top20_lin["plan"] = df_top20_lin.apply(
        lambda res: explain.parse_explain_analyze(res["query"], res["explain"], with_subqueries=False), axis="columns")
    df_top20_sq["plan"] = df_top20_sq.apply(
        lambda res: explain.parse_explain_analyze(res["query"], res["explain"], with_subqueries=True), axis="columns")
    df_top20_lin["subqueries"] = "linear"
    df_top20_sq["subqueries"] = "smart"

    df_top20 = pd.merge(df_top20_sq[["label", "query", "upper_bound", "explain", "plan", "execution_time"]],
                        df_top20_lin[["label", "query", "explain", "plan", "execution_time"]],
                        on="label", suffixes=("", "_linear"))
    df_top20["subquery_speedup"] = df_top20["execution_time_linear"] - df_top20["execution_time"]

    best_sq_speedup = df_top20[df_top20.subquery_speedup == df_top20.subquery_speedup.max()].iloc[0]

    report.next_section("Results for Figure 07 - Subquery Generation")
    report.add_entry(f"Query with largest performance improvement: {best_sq_speedup['label']}")
    report.add_entry(f"Runtime [s] without subqueries: {best_sq_speedup['execution_time_linear']}")
    report.add_entry(f"Runtime [s] with subqueries: {best_sq_speedup['execution_time']}")
    report.add_separator()
    report.add_entry("Linear execution plan:")
    report.add_entry(best_sq_speedup.plan_linear.pretty_print(as_string=True, include_exec_time=True))
    report.add_separator()
    report.add_entry("Execution plan with subqueries:")
    report.add_entry(best_sq_speedup.plan.pretty_print(as_string=True, include_exec_time=True))


def eval_03_idxnlj_operators(report: Report):
    df_base = selection.best_query_repetition(pd.read_csv("workloads/job-ues-results-base.csv"),
                                              group_cols="label", performance_col="query_rt_total")
    df_idxnlj = selection.best_query_repetition(pd.read_csv("workloads/job-ues-results-idxnlj.csv"),
                                                group_cols="label", performance_col="query_rt_total")
    df_base.rename(columns={"query_result": "explain", "query_rt_total": "execution_time"}, inplace=True)
    df_idxnlj.rename(columns={"query_result": "explain", "query_rt_total": "execution_time"}, inplace=True)

    df_speedup = selection.reorder(pd.merge(df_base[["label", "query", "execution_time", "explain"]],
                                            df_idxnlj[["label", "execution_time", "explain"]],
                                            on="label", suffixes=("_base", "_idxnlj")))
    df_speedup.set_index("label", inplace=True)
    df_speedup["query"] = df_speedup["query"].apply(mosp.MospQuery.parse)
    df_speedup["n_subqueries"] = df_speedup["query"].apply(lambda q: len(q.subqueries()))
    df_speedup.drop(df_speedup[df_speedup.n_subqueries == 0].index, inplace=True)

    df_speedup["idxnlj_speedup"] = df_speedup["execution_time_base"] - df_speedup["execution_time_idxnlj"]
    df_speedup["explain_base"] = df_speedup["explain_base"].apply(json.loads)
    df_speedup["explain_idxnlj"] = df_speedup["explain_idxnlj"].apply(json.loads)
    df_speedup["plan_base"] = df_speedup.apply(
        lambda res: explain.parse_explain_analyze(res["query"], res["explain_base"], with_subqueries=False),
        axis="columns")
    df_speedup["plan_idxnlj"] = df_speedup.apply(
        lambda res: explain.parse_explain_analyze(res["query"], res["explain_idxnlj"], with_subqueries=False),
        axis="columns")

    df_speedup = selection.reorder(pd.merge(df_base[["label", "query", "execution_time", "explain"]],
                                            df_idxnlj[["label", "execution_time", "explain"]],
                                            on="label", suffixes=("_base", "_idxnlj")))
    df_speedup.set_index("label", inplace=True)
    df_speedup["query"] = df_speedup["query"].apply(mosp.MospQuery.parse)
    df_speedup["n_subqueries"] = df_speedup["query"].apply(lambda q: len(q.subqueries()))
    df_speedup["idxnlj_speedup"] = df_speedup["execution_time_base"] - df_speedup["execution_time_idxnlj"]
    df_speedup["explain_base"] = df_speedup["explain_base"].apply(json.loads)
    df_speedup["explain_idxnlj"] = df_speedup["explain_idxnlj"].apply(json.loads)
    df_speedup["plan_base"] = df_speedup.apply(
        lambda res: explain.parse_explain_analyze(res["query"], res["explain_base"], with_subqueries=False),
        axis="columns")
    df_speedup["plan_idxnlj"] = df_speedup.apply(
        lambda res: explain.parse_explain_analyze(res["query"], res["explain_idxnlj"], with_subqueries=False),
        axis="columns")

    best_op_speedup = df_speedup[df_speedup.idxnlj_speedup == df_speedup.idxnlj_speedup.max()].iloc[0]

    report.next_section("Results for Figure 09 - IdxNLJ operator selection")
    report.add_entry(f"Query with largest performance improvement: {best_op_speedup.name}")
    report.add_entry(f"Runtime [s] with HashJoin: {best_op_speedup['execution_time_base']}")
    report.add_entry(f"Runtime [s] with IdxNLJ: {best_op_speedup['execution_time_idxnlj']}")
    report.add_separator()
    report.add_entry("Execution plan with HashJoin only:")
    report.add_entry(best_op_speedup.plan_base.pretty_print(as_string=True, include_exec_time=True))
    report.add_separator()
    report.add_entry("Execution plan with IdxNLJ:")
    report.add_entry(best_op_speedup.plan_idxnlj.pretty_print(as_string=True, include_exec_time=True))


def main():
    # Aggregate result files for Top-k settings
    results = aggregate_cautious()
    results_approx = aggregate_approx()
    results.to_csv("evaluation/job-ues-eval-topk-exhaustive.csv", index=False)
    results_approx.to_csv("evaluation/job-ues-eval-topk-approx.csv", index=False)

    # Analyze PostBOUND/UES/native results
    report = Report("../evaluation_report.txt")
    eval_01_join_orders(report)
    eval_02_subquery_generation(report)
    eval_03_idxnlj_operators(report)
    report.write()


if __name__ == "__main__":
    main()
