#!/usr/bin/env Rscript
args <- commandArgs(trailingOnly = TRUE)

infile <- "results/audit/reproducibility_checks.tsv"
outdir <- "plots/publication"

for (a in args) {
  if (startsWith(a, "--infile=")) infile <- sub("^--infile=", "", a)
  if (startsWith(a, "--outdir=")) outdir <- sub("^--outdir=", "", a)
}

out_pdf <- file.path(outdir, "pdf", "F2_reproducibility.pdf")
out_png <- file.path(outdir, "png", "F2_reproducibility.png")

dir.create(dirname(out_pdf), recursive = TRUE, showWarnings = FALSE)
dir.create(dirname(out_png), recursive = TRUE, showWarnings = FALSE)

if (!file.exists(infile)) stop(paste("Missing anchor table:", infile))

if (!requireNamespace("ggplot2", quietly = TRUE) || !requireNamespace("patchwork", quietly = TRUE)) {
  stop("Missing R deps: ggplot2 + patchwork")
}
suppressPackageStartupMessages(library(ggplot2))
suppressPackageStartupMessages(library(patchwork))
this_file <- NULL
for (a in commandArgs(trailingOnly = FALSE)) if (startsWith(a, "--file=")) this_file <- sub("^--file=", "", a)
this_dir <- if (is.null(this_file)) getwd() else dirname(normalizePath(this_file))
source(file.path(this_dir, "_cloudbio_theme.R"))

df <- read.delim(infile, sep = "\t", header = TRUE, stringsAsFactors = FALSE, check.names = FALSE)
if (nrow(df) == 0) stop("No rows in reproducibility_checks.tsv")

df$pass <- suppressWarnings(as.numeric(df$pass))
df$stage <- as.character(df$stage)
df$dataset_id <- as.character(df$dataset_id)
df$env_hash <- as.character(df$env_hash)
df$output_table_path <- as.character(df$output_table_path)
df$output_sha256 <- as.character(df$output_sha256)
n_checks_total <- nrow(df[is.finite(df$pass) & df$pass %in% c(0, 1), , drop = FALSE])

base_theme <- cloudbio_base_theme()
wrap_text <- function(x, width = 72) {
  if (is.null(x) || is.na(x) || !nzchar(x)) return(x)
  paste(base::strwrap(x, width = width), collapse = "\n")
}

wilson_ci <- function(k, n, z = 1.96) {
  if (n <= 0) return(c(NA, NA))
  p <- k / n
  den <- 1 + (z^2 / n)
  center <- (p + (z^2 / (2 * n))) / den
  half <- (z * sqrt((p * (1 - p) / n) + (z^2 / (4 * n^2)))) / den
  c(max(0, center - half), min(1, center + half))
}

# By stage summary
agg <- aggregate(pass ~ stage, data = df, FUN = function(x) c(k = sum(x == 1, na.rm = TRUE), n = length(x)))
agg$k <- agg$pass[, "k"]
agg$n <- agg$pass[, "n"]
agg$pass_rate <- ifelse(agg$n > 0, agg$k / agg$n, NA)
agg$stage_short <- gsub("_", " ", agg$stage)
agg$stage_short <- vapply(agg$stage_short, function(x) paste(strwrap(x, width = 22), collapse = "\n"), character(1))
cis <- t(mapply(wilson_ci, agg$k, agg$n))
agg$ci_low <- cis[, 1]
agg$ci_high <- cis[, 2]

p_stage <- ggplot(agg, aes(x = reorder(stage_short, pass_rate), y = pass_rate)) +
  geom_pointrange(aes(ymin = ci_low, ymax = ci_high), linewidth = 0.45, color = cloudbio_palette$accent) +
  geom_text(aes(label = paste0("n=", n)), hjust = -0.10, size = 3.0, color = "#333333") +
  coord_flip(clip = "off") +
  scale_y_continuous(limits = c(0, 1), expand = expansion(mult = c(0, 0.16))) +
  labs(
    title = paste0("Cross-environment stability by pipeline stage (n=", n_checks_total, " checks)"),
    subtitle = "Wilson 95% CI by declared stage under a pinned environment identifier",
    x = NULL,
    y = "Pass rate"
  ) +
  base_theme

# By dataset summary (exclude NA-only label noise)
df$dataset_label <- ifelse(is.na(df$dataset_id) | df$dataset_id == "" | df$dataset_id == "NA", "contract/skeleton", df$dataset_id)
ds <- aggregate(pass ~ dataset_label, data = df, FUN = function(x) c(k = sum(x == 1, na.rm = TRUE), n = length(x)))
ds$k <- ds$pass[, "k"]
ds$n <- ds$pass[, "n"]
ds$pass_rate <- ifelse(ds$n > 0, ds$k / ds$n, NA)
cis2 <- t(mapply(wilson_ci, ds$k, ds$n))
ds$ci_low <- cis2[, 1]
ds$ci_high <- cis2[, 2]
ds$dataset_short <- ds$dataset_label
ds$dataset_short <- gsub("^10x_", "", ds$dataset_short)
ds$dataset_short <- gsub("_scRNA_2016_S3$", " PBMC3k", ds$dataset_short)
ds$dataset_short <- gsub("_scRNA$", " scRNA", ds$dataset_short)
ds$dataset_short <- gsub("_Visium_10x$", " Visium", ds$dataset_short)
ds$dataset_short <- gsub("_", " ", ds$dataset_short)
ds$dataset_short <- vapply(ds$dataset_short, function(x) paste(strwrap(x, width = 20), collapse = "\n"), character(1))

p_dataset <- ggplot(ds, aes(x = reorder(dataset_short, pass_rate), y = pass_rate)) +
  geom_pointrange(aes(ymin = ci_low, ymax = ci_high), linewidth = 0.45, color = cloudbio_palette$accent) +
  geom_text(aes(label = paste0("n=", n)), hjust = -0.10, size = 3.0, color = "#333333") +
  coord_flip(clip = "off") +
  scale_y_continuous(limits = c(0, 1), expand = expansion(mult = c(0, 0.16))) +
  labs(
    title = "Cross-environment stability by dataset/task",
    subtitle = "Wilson 95% CI; \"contract/skeleton\" includes non-dataset checks",
    x = NULL,
    y = "Pass rate"
  ) +
  base_theme

# Output hash diversity across reruns (unique SHA-256 per output table)
hash_agg <- aggregate(output_sha256 ~ output_table_path, data = df, FUN = function(x) length(unique(x)))
colnames(hash_agg) <- c("output_table_path", "n_unique_hashes")
hash_agg$n_checks <- aggregate(pass ~ output_table_path, data = df, FUN = length)$pass
hash_agg$stable <- ifelse(hash_agg$n_unique_hashes == 1, "stable", "variant")
hash_agg$output_short <- sub("^results/", "", hash_agg$output_table_path)

p_hash <- ggplot(hash_agg, aes(x = n_unique_hashes, y = reorder(output_short, n_unique_hashes), color = stable, size = n_checks)) +
  geom_segment(aes(x = 0, xend = n_unique_hashes, yend = reorder(output_short, n_unique_hashes)), linewidth = 0.6, alpha = 0.6) +
  geom_point(alpha = 0.9) +
  geom_text(aes(label = paste0("hashes=", n_unique_hashes, ", n=", n_checks)), nudge_x = 1.10, hjust = 0, size = 2.7, color = "#333333") +
  scale_color_manual(values = c(stable = cloudbio_palette$ok, variant = cloudbio_palette$fail)) +
  scale_size_continuous(range = c(2.4, 6.2)) +
  labs(
    title = "Output hash stability across reruns",
    subtitle = wrap_text("Unique SHA-256 per anchored output table; pinning the environment identifier reduces hidden variation in declared outputs", width = 68),
    x = "Unique output hashes (count)",
    y = NULL,
    color = NULL,
    size = "# checks"
  ) +
  base_theme +
  theme(
    legend.position = "right",
    legend.key.height = grid::unit(12, "pt"),
    axis.text.y = element_text(size = 8.2),
    plot.margin = margin(10, 36, 14, 18, "pt")
  ) +
  guides(
    color = guide_legend(order = 1, override.aes = list(size = 3.8, alpha = 1)),
    size = guide_legend(order = 2, override.aes = list(alpha = 1))
  ) +
  scale_x_continuous(expand = expansion(mult = c(0, 0.40))) +
  coord_cartesian(clip = "off")

final <- ((p_stage / p_dataset) | p_hash) + patchwork::plot_layout(widths = c(1.22, 1))
final <- final + plot_annotation(tag_levels = "A")
final <- final & cloudbio_tag_theme()

ggplot2::ggsave(out_pdf, plot = final, width = 11.69, height = 8.27, units = "in", device = "pdf")
ggplot2::ggsave(out_png, plot = final, width = 11.69, height = 8.27, units = "in", dpi = 300)

cat(paste("Wrote:", out_pdf, "\n"))
cat(paste("Wrote:", out_png, "\n"))
