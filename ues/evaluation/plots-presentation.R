## plots.presentation.R
# Generates the plots used for our BTW'23 paper
#

library(readr)
library(dplyr, warn.conflicts = FALSE)
library(forcats)
library(tidyr)
library(stringr)

library(ggplot2)
library(viridis)
library(scales, warn.conflicts = FALSE)

options(readr.show_col_types = FALSE)

# - - - - - - - - - - - - - - - -
# 00: Basic setup ----
# - - - - - - - - - - - - - - - -
select_best_query_repetition <- function(result_path) {
  if(length(class(result_path)) == 1 && class(result_path) == "character") {
    df <- read_csv(result_path)
  } else {
    df <- result_path
  }
  representatives <- df %>% group_by(label) %>% arrange(query_rt_total) %>% slice_head() %>% ungroup()
  return(representatives)
}


# - - - - - - - - - - - - - - - -
# 01: UES overestimation ----
# - - - - - - - - - - - - - - - -
true_cards <- read_csv("workloads/job-results-true-cards.csv") %>% rename(true_card = query_result) %>% select(label, true_card)
ues_bounds <- read_csv("workloads/job-ues-workload-orig.csv") %>% rename(upper_bound = ues_final_bound) %>% select(label, upper_bound)
ues_overestimation <- inner_join(ues_bounds, true_cards, by = "label") %>%
  mutate(overestimation = (upper_bound+1) / (true_card+1)) %>%
  arrange(overestimation)

ggplot(ues_overestimation, aes(x = 1:nrow(ues_overestimation), y = overestimation)) +
  geom_point() +
  scale_y_log10() +
  labs(x = "Query (ordered by overestimation)", y = "Overestimation factor") +
  theme_bw() +
  theme(axis.text.x = element_blank(), text = element_text(size = 14))
ggsave("evaluation/plot-ues-overestimation.pdf")


# - - - - - - - - - - - - - - - -
# 03: Tighter upper bounds ----
# - - - - - - - - - - - - - - - -
read_gap_workload <- function(topk) {
  return(read_csv(str_glue("workloads/topk-setups/job-ues-workload-topk-{topk}-approx-linear.csv")) %>%
           rename(upper_bound = ues_final_bound) %>%
           mutate(mode = "top-k", subquery_mode = "linear", setting = str_glue("Top-{topk}"),
                  topk_length = topk, estimator = "approximate")
         )
}

workload_settings <- c("UES", str_c("Top-", c(1:5, 10, 20, 50, 100, 500)))
df_cautious <- read_csv("evaluation/job-ues-eval-topk-exhaustive.csv") %>% mutate(estimator = "cautious", subquery_mode = "smart")
df_approx <- read_csv("evaluation/job-ues-eval-topk-approx.csv") %>% mutate(estimator = "approximate")
approx_topk_gaps <- bind_rows(lapply(2:4, read_gap_workload))
df_topk <- bind_rows(df_cautious, df_approx, approx_topk_gaps) %>%
  mutate(setting = factor(setting, levels = workload_settings, ordered = TRUE))
true_cards <- read_csv("workloads/job-results-true-cards.csv")

# Upper bounds

median_ues_bound <- df_cautious %>% filter(mode == "ues") %>% summarise(med = median(upper_bound)) %>% pull()
median_topk_bounds <- df_topk %>%
  filter(mode == "top-k") %>%
  filter(subquery_mode == "linear" | estimator == "cautious") %>%
  group_by(setting, estimator) %>%
  summarise(median_upper_bound = median(upper_bound))
median_true_card <- true_cards %>% summarise(med = median(query_result)) %>% pull()

pdf("evaluation/plot-job-upper-bounds.pdf", width = 8, height = 5)
ggplot(median_topk_bounds, aes(x = setting, y = median_upper_bound, color = estimator, group = estimator, linetype = estimator)) +
  geom_point(size = 2) +
  geom_line(size = 1.2) +
  geom_hline(aes(yintercept = median_ues_bound, colour = "UES", linetype = "UES"), size = 1.2) +
  scale_y_log10() +
  scale_color_viridis(option = "cividis", discrete = TRUE, begin = 0.2, end = 0.95) +
  labs(x = "Workload", y = "Median upper bound", color = "Bound formula", linetype = "Bound formula", fill = "Bound formula") +
  theme_bw() +
  theme(text= element_text(size = 20), axis.text.x = element_text(angle = 25, hjust = 1))
dev.off()

# Max bound reduction
ues_improvement <- inner_join(
  df_topk %>%
    filter(mode == "top-k") %>%
    filter(subquery_mode == "linear" | estimator == "cautious") %>%
    select(label, setting, estimator, upper_bound),
  df_topk %>%
    filter(mode == "ues", subquery_mode == "smart", estimator == "approximate") %>%
    select(label, upper_bound),
  by = "label", suffix = c("_topk", "_ues")) %>%
  mutate(ues_improvement = upper_bound_ues / upper_bound_topk)
ues_improvement %>% slice_max(ues_improvement)

# Optimization time

ues_optimization_time <- df_cautious %>% filter(mode == "ues") %>% summarise(opt = sum(optimization_time)) %>% pull()
topk_optimization_time <- df_topk %>%
  filter(mode == "top-k") %>%
  filter(subquery_mode == "linear" | estimator == "cautious") %>%
  group_by(setting, estimator) %>%
  summarise(optimization_time = sum(optimization_time))

pdf("evaluation/plot-job-optimization-time.pdf", width = 8, height = 5)
ggplot(topk_optimization_time, aes(x = setting, y = optimization_time, group = estimator, color = estimator, linetype = estimator)) +
  geom_point(size = 2) +
  geom_line(size = 1.2) +
  geom_hline(aes(yintercept = ues_optimization_time, linetype = "UES", colour = "UES"), size = 1.2) +
  scale_color_viridis(option = "cividis", discrete = TRUE, end = 0.95) +
  labs(x = "Workload", y = "Optimization time [in sec.]", color = "Bound formula", linetype = "Bound formula") +
  theme_bw() +
  theme(text= element_text(size = 20), axis.text.x = element_text(angle = 25, hjust = 1))
dev.off()
