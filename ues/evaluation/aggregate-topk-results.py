#!/usr/bin/env python3

import json

import numpy as np
import pandas as pd

from analysis import selection
from transform import mosp


def read_workload(topk_length: int = np.nan, raw: str = "") -> pd.DataFrame:
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
    all_workloads = [read_workload(raw="workloads/job-ues-results-base-smart.csv")]
    for topk_setting in range(1, 6):
        all_workloads.append(read_workload(topk_setting))
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
    all_workloads_approx = [read_workload_approx(raw="workloads/job-ues-results-base-smart.csv"), read_workload_approx(raw="workloads/job-ues-results-base-linear.csv", linear=True)]
    for topk_setting in [1, 5, 10, 20, 50, 100, 500]:
        all_workloads_approx.append(read_workload_approx(topk_setting))
        all_workloads_approx.append(read_workload_approx(topk_setting, linear=True))
    results_approx = pd.concat(all_workloads_approx).reset_index().merge(job_cards, on="label")
    results_approx["overestimation"] = (results_approx["upper_bound"] + 1) / (results_approx["true_card"] + 1)
    results_approx["n_subqueries"] = results_approx["query"].apply(lambda q: len(q.subqueries()))
    results_approx["setting"] = np.where(results_approx["mode"] == "ues", "UES", "Top-" + results_approx["topk_length"].astype(str))
    settings_approx = pd.Categorical(results_approx["setting"], categories=["UES"] + list(map(lambda k: f"Top-{k}", sorted(filter(lambda k: k > 0, results_approx.topk_length.unique())))), ordered=True)
    results_approx["setting"] = settings_approx
    results_approx.drop(columns=["optimization_success", "ues_bounds", "query_result", "run"], inplace=True)
    return results_approx


def main():
    results = aggregate_cautious()
    results_approx = aggregate_approx()
    results.to_csv("evaluation/job-ues-eval-topk-exhaustive.csv", index=False)
    results_approx.to_csv("evaluation/job-ues-eval-topk-approx.csv", index=False)


if __name__ == "__main__":
    main()
