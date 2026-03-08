#!/usr/bin/env Rscript
args <- commandArgs(trailingOnly = TRUE)

infile <- "results/benchmarks/robustness_matrix.tsv"
outdir <- "plots/publication"

for (a in args) {
  if (startsWith(a, "--infile=")) infile <- sub("^--infile=", "", a)
  if (startsWith(a, "--outdir=")) outdir <- sub("^--outdir=", "", a)
}

out_pdf <- file.path(outdir, "pdf", "F6_robustness_matrix.pdf")
out_png <- file.path(outdir, "png", "F6_robustness_matrix.png")

dir.create(dirname(out_pdf), recursive = TRUE, showWarnings = FALSE)
dir.create(dirname(out_png), recursive = TRUE, showWarnings = FALSE)

if (!file.exists(infile)) stop(paste("Missing anchor table:", infile))

if (!requireNamespace("ggplot2", quietly = TRUE) || !requireNamespace("patchwork", quietly = TRUE) || !requireNamespace("tidyr", quietly = TRUE)) {
  stop("Missing R deps: ggplot2 + patchwork + tidyr")
}
suppressPackageStartupMessages(library(ggplot2))
suppressPackageStartupMessages(library(patchwork))
suppressPackageStartupMessages(library(tidyr))
this_file <- NULL
for (a in commandArgs(trailingOnly = FALSE)) if (startsWith(a, "--file=")) this_file <- sub("^--file=", "", a)
this_dir <- if (is.null(this_file)) getwd() else dirname(normalizePath(this_file))
source(file.path(this_dir, "_cloudbio_theme.R"))

df <- read.delim(infile, sep = "\t", header = TRUE, stringsAsFactors = FALSE, check.names = FALSE)
if (nrow(df) == 0) stop("No rows in robustness_matrix.tsv")

df$pass <- suppressWarnings(as.numeric(df$pass))
df$metric_value <- suppressWarnings(as.numeric(df$metric_value))
df$delta_vs_nominal <- suppressWarnings(as.numeric(df$delta_vs_nominal))

df$dataset_id <- as.character(df$dataset_id)
df$modality <- as.character(df$modality)
df$method_id <- as.character(df$method_id)
df$perturbation_id <- as.character(df$perturbation_id)
df$severity <- as.character(df$severity)
df$metric_id <- as.character(df$metric_id)
df$failure_reason <- as.character(df$failure_reason)
n_eval_total <- nrow(df)
n_fail_total <- sum(df$pass == 0, na.rm = TRUE)

df$method_clean <- gsub("^baseline:", "", df$method_id)
df$scenario <- paste0(df$perturbation_id, " (", df$severity, ")")
df$scenario <- ifelse(is.na(df$severity) | df$severity == "" | df$severity == "na", df$perturbation_id, df$scenario)
df$scenario <- gsub("^seed_plus_1", "seed+1", df$scenario)
df$scenario <- gsub("^hvg_half", "HVG/2", df$scenario)
df$scenario <- gsub("^integration_vs_baseline", "integration-vs-baseline", df$scenario)

df$dataset_short <- df$dataset_id
df$dataset_short <- gsub("^PBMC3K_PLUS_PBMC10K_INTEGRATION$", "PBMC3k+10k", df$dataset_short)
df$dataset_short <- gsub("^10x_PBMC3K_PBMC10K_V3_INTEGRATION_PAIR_S3$", "PBMC3k+10k pair", df$dataset_short)
df$dataset_short <- gsub("^10x_PBMC_10k_scRNA$", "PBMC 10k scRNA", df$dataset_short)
df$dataset_short <- gsub("^10x_PBMC_3k_scRNA_2016_S3$", "PBMC 3k scRNA", df$dataset_short)
df$dataset_short <- gsub("^Mouse_Brain_Visium_10x$", "Mouse Brain Visium", df$dataset_short)
df$dataset_short <- gsub("^10x_", "", df$dataset_short)
df$dataset_short <- gsub("_scRNA_2016_S3$", " PBMC3k", df$dataset_short)
df$dataset_short <- gsub("_scRNA$", " scRNA", df$dataset_short)
df$dataset_short <- gsub("_Visium_10x$", " Visium", df$dataset_short)

df$metric_short <- df$metric_id
df$metric_short <- gsub("^ARI_cluster$", "ARI_cluster", df$metric_short)
df$metric_short <- gsub("^mean_pearson_by_cell_type$", "mean_pearson", df$metric_short)
df$row_label <- paste0(df$dataset_short, "\n", df$scenario, " | ", df$metric_short)
df$row_label <- vapply(df$row_label, function(x) paste(strwrap(x, width = 34), collapse = "\n"), character(1))

df$pass_label <- ifelse(df$pass == 1, "PASS", ifelse(df$pass == 0, "FAIL", NA))

# Aggregate in case of duplicate cells
cell <- aggregate(cbind(pass, delta_vs_nominal) ~ method_clean + row_label, data = df, FUN = function(x) {
  if (all(is.na(x))) return(NA)
  x
})
cell$pass <- sapply(cell$pass, function(x) if (length(x) == 0) NA else suppressWarnings(as.numeric(min(x, na.rm = TRUE))))
cell$delta_vs_nominal <- sapply(cell$delta_vs_nominal, function(x) if (length(x) == 0) NA else suppressWarnings(as.numeric(mean(x, na.rm = TRUE))))

methods <- sort(unique(df$method_clean))
rows <- unique(df$row_label)
grid <- tidyr::expand_grid(method_clean = methods, row_label = rows)
grid <- merge(grid, cell[, c("method_clean", "row_label", "pass", "delta_vs_nominal")], by = c("method_clean", "row_label"), all.x = TRUE)
grid$pass_label <- ifelse(grid$pass == 1, "PASS", ifelse(grid$pass == 0, "FAIL", "NA"))
grid$delta_txt <- ""
sel <- !is.na(grid$delta_vs_nominal)
grid$delta_txt[sel] <- sprintf("%+.3f", grid$delta_vs_nominal[sel])

base_theme <- cloudbio_base_theme() + theme(plot.margin = margin(10, 18, 14, 18, "pt"))

p_heat <- ggplot(grid, aes(x = method_clean, y = row_label, fill = pass_label)) +
  geom_tile(color = "white", linewidth = 0.3) +
  scale_fill_manual(
    values = c(PASS = "#1B9E77", FAIL = "#D95F02", "NA" = "#BDBDBD"),
    breaks = c("FAIL", "PASS", "NA"),
    labels = c("Stability threshold not met", "PASS", "NA")
  ) +
  labs(
    title = "Algorithmic sensitivity mapping under predeclared perturbations",
    subtitle = paste0(
      "Tile color = threshold result; text shows delta vs nominal ",
      "(ARI threshold = 0.90; Tangram HVG/2 Pearson threshold = 0.90; ",
      n_fail_total, "/", n_eval_total, " failures)"
    ),
    x = NULL,
    y = NULL,
    fill = NULL
  ) +
  base_theme +
  theme(
    axis.text.x = element_text(angle = 35, hjust = 1),
    axis.text.y = element_text(size = 7.0, lineheight = 1.12)
  )

p_heat <- p_heat +
  geom_text(aes(label = delta_txt), size = 2.2, color = "black")

# Seed sensitivity (determinism) summary: seed vs seed+1
seed_df <- df[df$perturbation_id == "seed_plus_1" & grepl("^ARI", df$metric_id) & is.finite(df$metric_value), ]
summarize_iqr <- function(v) {
  v <- v[is.finite(v)]
  if (length(v) == 0) return(c(med = NA, q25 = NA, q75 = NA, n = 0))
  c(med = stats::median(v), q25 = as.numeric(stats::quantile(v, 0.25)), q75 = as.numeric(stats::quantile(v, 0.75)), n = length(v))
}

if (nrow(seed_df) > 0) {
  seed_df$dataset_short <- seed_df$dataset_id
  seed_df$dataset_short <- gsub("^10x_", "", seed_df$dataset_short)
  seed_df$dataset_short <- gsub("_scRNA_2016_S3$", " PBMC3k", seed_df$dataset_short)
  seed_df$dataset_short <- gsub("_scRNA$", " scRNA", seed_df$dataset_short)

  seed_sum <- aggregate(metric_value ~ dataset_short + method_clean, data = seed_df, FUN = summarize_iqr)
  seed_sum$med <- seed_sum$metric_value[, "med"]
  seed_sum$q25 <- seed_sum$metric_value[, "q25"]
  seed_sum$q75 <- seed_sum$metric_value[, "q75"]
  seed_sum$n <- seed_sum$metric_value[, "n"]

  p_seed <- ggplot(seed_sum, aes(x = med, y = reorder(method_clean, med), color = dataset_short)) +
    geom_vline(xintercept = 0.95, linewidth = 0.35, linetype = "dashed", color = "grey60") +
    geom_pointrange(aes(xmin = q25, xmax = q75), linewidth = 0.55) +
    geom_point(size = 2.2) +
    geom_text(aes(label = paste0("n=", n)), hjust = -0.10, size = 3.0, color = "#333333") +
    scale_x_continuous(limits = c(0.90, 1.00), breaks = c(0.90, 0.95, 1.00), expand = expansion(mult = c(0, 0.16))) +
    labs(
      title = "Seed sensitivity (determinism check)",
      subtitle = "ARI under seed vs seed+1 perturbation (median and IQR across reruns)",
      x = "ARI (higher = more stable)",
      y = NULL,
      color = NULL
    ) +
    base_theme +
    theme(
      plot.title = element_text(size = 11, face = "bold"),
      plot.subtitle = element_text(size = 9, lineheight = 1.05),
      plot.margin = margin(10, 34, 14, 18, "pt")
    ) +
    coord_cartesian(clip = "off")
} else {
  p_seed <- ggplot() + geom_blank() + labs(
    title = "Seed sensitivity (determinism check)",
    subtitle = "No seed_plus_1 rows found in robustness_matrix.tsv"
  ) + base_theme
}

# Failure reasons summary
fail_df <- df[df$pass == 0, ]
if (nrow(fail_df) > 0) {
  fail_df$failure_reason[is.na(fail_df$failure_reason) | fail_df$failure_reason == ""] <- "Unspecified"
  agg <- aggregate(method_clean ~ failure_reason, data = fail_df, FUN = length)
  colnames(agg) <- c("failure_reason", "n")
  agg <- agg[order(-agg$n), ]
  agg <- head(agg, 10)
  p_fail <- ggplot(agg, aes(x = reorder(failure_reason, n), y = n)) +
    geom_segment(aes(xend = failure_reason, y = 0, yend = n), linewidth = 0.7, color = cloudbio_palette$fail, alpha = 0.85) +
    geom_point(size = 2.6, color = cloudbio_palette$fail, alpha = 0.95) +
    geom_text(aes(label = n), hjust = -0.10, size = 3.0, color = "#333333") +
    coord_flip() +
    scale_y_continuous(expand = expansion(mult = c(0, 0.18))) +
    labs(
      title = "Top failure reasons (failed cells only)",
      subtitle = "Counts of failure_reason in robustness_matrix.tsv",
      x = NULL,
      y = "Count"
    ) +
    base_theme
} else {
  p_fail <- ggplot() + geom_blank() + labs(title = "Top failure reasons", subtitle = "No failed cells in the current table") + base_theme
}

top_row <- (p_heat | p_seed) + patchwork::plot_layout(widths = c(1.34, 1.00))
final <- top_row / p_fail
final <- final + plot_annotation(tag_levels = "A")
final <- final & cloudbio_tag_theme()

ggplot2::ggsave(out_pdf, plot = final, width = 11.69, height = 8.27, units = "in", device = "pdf")
ggplot2::ggsave(out_png, plot = final, width = 11.69, height = 8.27, units = "in", dpi = 300)

cat(paste("Wrote:", out_pdf, "\n"))
cat(paste("Wrote:", out_png, "\n"))
