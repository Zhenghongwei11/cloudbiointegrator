#!/usr/bin/env Rscript
args <- commandArgs(trailingOnly = TRUE)

infile <- "results/benchmarks/runtime_cost_failure.tsv"
outdir <- "plots/publication"

for (a in args) {
  if (startsWith(a, "--infile=")) infile <- sub("^--infile=", "", a)
  if (startsWith(a, "--outdir=")) outdir <- sub("^--outdir=", "", a)
}

out_pdf <- file.path(outdir, "pdf", "F5_ops_benchmark.pdf")
out_png <- file.path(outdir, "png", "F5_ops_benchmark.png")

dir.create(dirname(out_pdf), recursive = TRUE, showWarnings = FALSE)
dir.create(dirname(out_png), recursive = TRUE, showWarnings = FALSE)

if (!file.exists(infile)) stop(paste("Missing anchor table:", infile))

if (!requireNamespace("ggplot2", quietly = TRUE) ||
    !requireNamespace("patchwork", quietly = TRUE) ||
    !requireNamespace("ggrepel", quietly = TRUE)) {
  stop("Missing R deps: ggplot2 + patchwork + ggrepel")
}
suppressPackageStartupMessages(library(ggplot2))
suppressPackageStartupMessages(library(patchwork))
suppressPackageStartupMessages(library(stringr))
this_file <- NULL
for (a in commandArgs(trailingOnly = FALSE)) if (startsWith(a, "--file=")) this_file <- sub("^--file=", "", a)
this_dir <- if (is.null(this_file)) getwd() else dirname(normalizePath(this_file))
source(file.path(this_dir, "_cloudbio_theme.R"))

df <- read.delim(infile, sep = "\t", header = TRUE, stringsAsFactors = FALSE, check.names = FALSE)
if (nrow(df) == 0) stop("No rows in runtime_cost_failure.tsv")

df$status <- as.character(df$status)
df$method_id <- as.character(df$method_id)
df$dataset_id <- as.character(df$dataset_id)
df$modality <- as.character(df$modality)
df$failure_type <- as.character(df$failure_type)

df$wall_time_s <- suppressWarnings(as.numeric(df$wall_time_s))
df$peak_ram_gb <- suppressWarnings(as.numeric(df$peak_ram_gb))
df$estimated_cost_usd <- suppressWarnings(as.numeric(df$estimated_cost_usd))

# Keep manuscript-consistent terminal outcomes only.
# The paper reports reliability over {ok, fail}; "skip" is not a terminal execution failure.
df <- df[df$status %in% c("ok", "fail"), , drop = FALSE]
if (nrow(df) == 0) stop("No {ok, fail} rows in runtime_cost_failure.tsv")
n_runs_total <- nrow(df)

df$ok <- ifelse(df$status == "ok", 1, 0)
df$method_clean <- gsub("^baseline:", "", df$method_id)
df$method_clean <- gsub("^advanced:", "", df$method_clean)

method_display <- df$method_clean
method_display <- gsub("^deconvolution:", "", method_display)
method_display <- gsub("^scanpy-visium-baseline$", "scanpy (Visium)", method_display)
method_display <- gsub("^scanpy-standard$", "scanpy (scRNA)", method_display)
method_display <- gsub("^seurat-v5-standard$", "Seurat v5", method_display)
method_display <- gsub("^python-ingest$", "ingest", method_display)
method_display <- gsub("^scvi$", "scVI", method_display)
method_display <- gsub("^harmony$", "Harmony", method_display)
method_display <- stringr::str_wrap(method_display, width = 11)
df$method_display <- method_display

base_theme <- cloudbio_base_theme()
base_theme_xrot <- base_theme + theme(axis.text.x = element_text(angle = 28, hjust = 1, size = 8.1))

# Panel A: wall time distribution (log10 seconds)
df_time <- df[!is.na(df$wall_time_s) & df$wall_time_s > 0, ]
method_order <- names(sort(tapply(df_time$wall_time_s, df_time$method_clean, stats::median, na.rm = TRUE)))
if (length(method_order) > 0) df_time$method_clean <- factor(df_time$method_clean, levels = method_order)
p_time <- ggplot(df_time, aes(x = method_clean, y = wall_time_s, color = status)) +
  geom_boxplot(outlier.shape = NA, linewidth = 0.3, alpha = 0.15) +
  geom_jitter(width = 0.12, height = 0, size = 1.1, alpha = 0.75) +
  scale_y_log10() +
  scale_color_manual(values = c(ok = cloudbio_palette$ok, fail = cloudbio_palette$fail)) +
  labs(
    title = paste0("Computational efficiency: runtime across runs (n=", n_runs_total, " runs)"),
    subtitle = "Per-run wall time (log10 seconds); points show individual runs",
    x = NULL,
    y = "Wall time (s, log10)",
    color = NULL
  ) +
  base_theme_xrot +
  theme(axis.title.y = element_text(margin = margin(r = 11))) +
  scale_x_discrete(labels = function(x) {
    # Map from method_clean to method_display (first match)
    out <- x
    for (i in seq_along(x)) {
      m <- df$method_display[df$method_clean == x[i]]
      if (length(m) > 0) out[i] <- m[1]
    }
    out
  })

# Panel B: failure evidence (per-run points + Wilson CI summary)
agg_fail <- aggregate(ok ~ method_clean, data = df, FUN = function(x) c(mean = mean(x), n = length(x), k = sum(x)))
agg_fail$fail_rate <- 1 - agg_fail$ok[, "mean"]
agg_fail$n <- agg_fail$ok[, "n"]
agg_fail$k_ok <- agg_fail$ok[, "k"]
agg_fail$k_fail <- agg_fail$n - agg_fail$k_ok

wilson_ci <- function(k, n, z = 1.96) {
  if (n <= 0) return(c(NA, NA))
  p <- k / n
  den <- 1 + (z^2 / n)
  center <- (p + (z^2 / (2 * n))) / den
  half <- (z * sqrt((p * (1 - p) / n) + (z^2 / (4 * n^2)))) / den
  c(max(0, center - half), min(1, center + half))
}

cis <- t(mapply(wilson_ci, agg_fail$k_fail, agg_fail$n))
agg_fail$ci_low <- cis[, 1]
agg_fail$ci_high <- cis[, 2]

df_fail_pts <- df
df_fail_pts$fail01 <- ifelse(df_fail_pts$status == "ok", 0, 1)

p_fail <- ggplot() +
  geom_jitter(
    data = df_fail_pts,
    aes(x = method_clean, y = fail01),
    width = 0.12,
    height = 0,
    size = 0.9,
    alpha = 0.55,
    color = cloudbio_palette$accent
  ) +
  geom_pointrange(
    data = agg_fail,
    aes(x = method_clean, y = fail_rate, ymin = ci_low, ymax = ci_high),
    linewidth = 0.45,
    color = cloudbio_palette$blue_dark
  ) +
  geom_text(data = agg_fail, aes(x = method_clean, y = pmin(fail_rate + 0.07, 1.04), label = paste0("n=", n)), size = 2.7) +
  scale_y_continuous(limits = c(0, 1.08), breaks = c(0, 0.5, 1.0), expand = expansion(mult = c(0, 0.02))) +
  labs(
    title = "Reliability: failure rate by method",
    subtitle = "Per-method failure rate with Wilson 95% CI (terminal outcomes: ok/fail)",
    x = NULL,
    y = "Failure rate"
  ) +
  base_theme_xrot +
  theme(axis.title.y = element_text(margin = margin(r = 10))) +
  scale_x_discrete(labels = function(x) {
    out <- x
    for (i in seq_along(x)) {
      m <- df$method_display[df$method_clean == x[i]]
      if (length(m) > 0) out[i] <- m[1]
    }
    out
  })

# Panel C: failure types (top 6; fail only, aligned with manuscript definition)
df_fail <- df[df$status == "fail", ]
if (nrow(df_fail) > 0) {
  df_fail$failure_type[is.na(df_fail$failure_type) | df_fail$failure_type == ""] <- "Unknown"
  agg_ft <- aggregate(run_id ~ failure_type, data = df_fail, FUN = length)
  colnames(agg_ft) <- c("failure_type", "n")
  agg_ft <- agg_ft[order(-agg_ft$n), ]
  top_k <- 6
  keep <- head(agg_ft$failure_type, top_k)
  df_fail$failure_type2 <- ifelse(df_fail$failure_type %in% keep, df_fail$failure_type, "Other")
  agg_ft2 <- aggregate(run_id ~ failure_type2, data = df_fail, FUN = length)
  colnames(agg_ft2) <- c("failure_type", "n")

  p_ft <- ggplot(agg_ft2, aes(x = reorder(failure_type, n), y = n)) +
    geom_segment(aes(xend = failure_type, y = 0, yend = n), linewidth = 0.7, color = "#E45756", alpha = 0.85) +
    geom_point(size = 2.6, color = "#E45756", alpha = 0.95) +
    coord_flip() +
    labs(
      title = "Observed failure types",
      subtitle = "Counts across failed runs (status=fail)",
      x = NULL,
      y = "Failed runs (count)"
    ) +
    base_theme
} else {
  p_ft <- ggplot() + geom_blank() + labs(title = "Observed failure types", subtitle = "No failures observed in the current table") + base_theme
}

# Panel D: runtime–reliability trade-off (avoids empty placeholder panels)
agg_time <- aggregate(wall_time_s ~ method_clean, data = df_time, FUN = function(v) {
  v <- v[is.finite(v) & v > 0]
  if (length(v) == 0) return(c(med = NA, q25 = NA, q75 = NA))
  c(med = stats::median(v), q25 = as.numeric(stats::quantile(v, 0.25)), q75 = as.numeric(stats::quantile(v, 0.75)))
})
agg_time$rt_med <- agg_time$wall_time_s[, "med"]
agg_time$rt_q25 <- agg_time$wall_time_s[, "q25"]
agg_time$rt_q75 <- agg_time$wall_time_s[, "q75"]

trade <- merge(agg_fail[, c("method_clean", "fail_rate", "ci_low", "ci_high", "n")], agg_time[, c("method_clean", "rt_med", "rt_q25", "rt_q75")], by = "method_clean", all = TRUE)
trade$method_display_one <- as.character(trade$method_clean)
for (i in seq_len(nrow(trade))) {
  m <- df$method_display[df$method_clean == trade$method_clean[i]]
  if (length(m) > 0) trade$method_display_one[i] <- as.character(m[1])
}
trade$label <- ""
if (nrow(trade) > 0) {
  idx_fail <- head(order(-trade$fail_rate), 3)
  idx_slow <- head(order(-trade$rt_med), 2)
  idx_fast <- head(order(trade$rt_med), 1)
  keep <- unique(c(idx_fail, idx_slow, idx_fast))
  keep <- keep[is.finite(keep) & keep >= 1 & keep <= nrow(trade)]
  trade$label[keep] <- trade$method_display_one[keep]
}

p_trade <- ggplot(trade, aes(x = rt_med, y = fail_rate)) +
  geom_errorbar(aes(ymin = ci_low, ymax = ci_high), width = 0, linewidth = 0.45, color = cloudbio_palette$blue_dark, alpha = 0.9) +
  geom_segment(aes(x = rt_q25, xend = rt_q75, y = fail_rate, yend = fail_rate), linewidth = 0.45, color = cloudbio_palette$blue_dark, alpha = 0.9) +
  geom_point(aes(size = n), color = cloudbio_palette$accent, alpha = 0.9) +
  ggrepel::geom_text_repel(
    data = trade[trade$label != "", , drop = FALSE],
    aes(label = label),
    size = 2.55,
    max.overlaps = Inf,
    box.padding = 0.8,
    point.padding = 0.35,
    min.segment.length = 0,
    segment.alpha = 0.65,
    force = 1.2,
    seed = 42
  ) +
  scale_x_log10() +
  scale_size_continuous(range = c(1.6, 4.2)) +
  labs(
    title = "Runtime–reliability trade-off",
    subtitle = paste0("Median runtime (x; IQR) vs failure rate (y; Wilson 95% CI) over {ok, fail} runs (n=", n_runs_total, ")"),
    x = "Runtime (s, log10)",
    y = "Failure rate",
    size = "# runs"
  ) +
  base_theme

final <- (p_time / p_fail) | (p_ft / p_trade)
final <- final + plot_annotation(tag_levels = "A")
final <- final & cloudbio_tag_theme()

ggplot2::ggsave(out_pdf, plot = final, width = 11.69, height = 8.27, units = "in", device = "pdf")
ggplot2::ggsave(out_png, plot = final, width = 11.69, height = 8.27, units = "in", dpi = 300)

cat(paste("Wrote:", out_pdf, "\n"))
cat(paste("Wrote:", out_png, "\n"))
