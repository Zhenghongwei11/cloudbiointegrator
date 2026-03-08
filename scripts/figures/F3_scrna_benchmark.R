#!/usr/bin/env Rscript
args <- commandArgs(trailingOnly = TRUE)

infile <- "results/benchmarks/method_benchmark.tsv"
concord_infile <- "results/benchmarks/biological_output_concordance.tsv"
outdir <- "plots/publication"
dataset_visium <- "Mouse_Brain_Visium_10x"
rctd_tsv <- "results/figures/visium_celltype_weights_rctd.tsv"
tangram_tsv <- "results/figures/visium_celltype_weights_tangram.tsv"
cell2_tsv <- "results/figures/visium_celltype_weights_cell2location.tsv"

for (a in args) {
  if (startsWith(a, "--infile=")) infile <- sub("^--infile=", "", a)
  if (startsWith(a, "--concord=")) concord_infile <- sub("^--concord=", "", a)
  if (startsWith(a, "--outdir=")) outdir <- sub("^--outdir=", "", a)
  if (startsWith(a, "--visium-dataset=")) dataset_visium <- sub("^--visium-dataset=", "", a)
  if (startsWith(a, "--rctd=")) rctd_tsv <- sub("^--rctd=", "", a)
  if (startsWith(a, "--tangram=")) tangram_tsv <- sub("^--tangram=", "", a)
  if (startsWith(a, "--cell2location=")) cell2_tsv <- sub("^--cell2location=", "", a)
}

out_pdf <- file.path(outdir, "pdf", "F3_scrna_benchmark.pdf")
out_png <- file.path(outdir, "png", "F3_scrna_benchmark.png")

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

wrap_text <- function(x, width = 88) {
  if (is.null(x) || is.na(x) || !nzchar(x)) return(x)
  paste(base::strwrap(x, width = width), collapse = "\n")
}

pretty_visium_label <- function(dataset_id) {
  if (is.na(dataset_id) || !nzchar(dataset_id)) return("Visium dataset")
  if (dataset_id == "Mouse_Brain_Visium_10x") return("Mouse brain Visium")
  if (grepl("lymph", dataset_id, ignore.case = TRUE)) return("Human lymph node Visium")
  gsub("_", " ", gsub("^10x_", "", dataset_id))
}

df <- read.delim(infile, sep = "\t", header = TRUE, stringsAsFactors = FALSE, check.names = FALSE)
if (nrow(df) == 0) stop("No rows in method_benchmark.tsv")

df$modality <- as.character(df$modality)
df$dataset_id <- as.character(df$dataset_id)
df$method_id <- as.character(df$method_id)
df$task <- as.character(df$task)
df$metric_id <- as.character(df$metric_id)
df$metric_value <- suppressWarnings(as.numeric(df$metric_value))
df$baseline_flag <- suppressWarnings(as.numeric(df$baseline_flag))
df$replicate_id <- as.character(df$replicate_id)

sc <- df[df$modality == "scRNA-seq" & !is.na(df$metric_value), ]
if (nrow(sc) == 0) stop("No scRNA-seq rows with numeric metric_value")

sc$method_clean <- gsub("^baseline:", "", sc$method_id)

base_theme <- cloudbio_base_theme() +
  theme(
    strip.text = element_text(size = 9.2, face = "bold", lineheight = 1.05, margin = margin(b = 4)),
    panel.spacing = grid::unit(7, "pt")
  )

concordance_caption <- function(concord_path, dataset_id) {
  if (is.null(concord_path) || !file.exists(concord_path)) return(NULL)
  cc <- utils::read.delim(concord_path, sep = "\t", header = TRUE, stringsAsFactors = FALSE, check.names = FALSE)
  req <- c("dataset_id", "concordance_metric", "value", "notes")
  if (!all(req %in% colnames(cc))) return(NULL)
  cc <- cc[cc$dataset_id == dataset_id & cc$concordance_metric == "median_cosine_by_spot", , drop = FALSE]
  if (nrow(cc) == 0) return(NULL)

  fmt <- function(x) {
    x <- suppressWarnings(as.numeric(x))
    if (!is.finite(x)) return("NA")
    sprintf("%.2f", x)
  }

  # Expect notes tokens like "pair=rctd_vs_tangram; ..."
  pairs <- c("rctd_vs_tangram", "tangram_vs_cell2location", "rctd_vs_cell2location")
  pair_labels <- c(
    rctd_vs_tangram = "RCTD/Tangram",
    tangram_vs_cell2location = "Tangram/cell2location",
    rctd_vs_cell2location = "RCTD/cell2location"
  )
  chunks <- c()
  for (p in pairs) {
    row <- cc[grepl(paste0("pair=", p), cc$notes, fixed = FALSE), , drop = FALSE]
    if (nrow(row) < 1) next
    row <- row[1, , drop = FALSE]
    ci_low <- if ("ci_low" %in% colnames(row)) fmt(row$ci_low) else "NA"
    ci_high <- if ("ci_high" %in% colnames(row)) fmt(row$ci_high) else "NA"
    pair_name <- if (!is.na(pair_labels[[p]])) pair_labels[[p]] else p
    chunks <- c(chunks, paste0(pair_name, "=", fmt(row$value), " [", ci_low, ",", ci_high, "]"))
  }
  if (length(chunks) == 0) return(NULL)
  wrap_text(
    paste(
      "Cross-method concordance (spotwise cosine):",
      paste(chunks, collapse = "; ")
    ),
    width = 78
  )
}

concordance_badge <- function(concord_path, dataset_id, pair_id = "rctd_vs_tangram") {
  if (is.null(concord_path) || !file.exists(concord_path)) return(NULL)
  cc <- utils::read.delim(concord_path, sep = "\t", header = TRUE, stringsAsFactors = FALSE, check.names = FALSE)
  req <- c("dataset_id", "concordance_metric", "value", "notes")
  if (!all(req %in% colnames(cc))) return(NULL)
  cc <- cc[cc$dataset_id == dataset_id & cc$concordance_metric == "median_cosine_by_spot", , drop = FALSE]
  if (nrow(cc) == 0) return(NULL)
  row <- cc[grepl(paste0("pair=", pair_id), cc$notes, fixed = FALSE), , drop = FALSE]
  if (nrow(row) < 1) return(NULL)
  row <- row[1, , drop = FALSE]

  fmt <- function(x) {
    x <- suppressWarnings(as.numeric(x))
    if (!is.finite(x)) return("NA")
    sprintf("%.2f", x)
  }

  v <- fmt(row$value)
  lo <- if ("ci_low" %in% colnames(row)) fmt(row$ci_low) else "NA"
  hi <- if ("ci_high" %in% colnames(row)) fmt(row$ci_high) else "NA"
  wrap_text(
    paste0("RCTD vs Tangram agreement: median cosine=", v, " (IQR ", lo, "-", hi, ")"),
    width = 64
  )
}

spotwise_cosine_summary <- function(a_path, b_path, dataset_id, a_label, b_label, b_strip_prefix = NULL) {
  if (!file.exists(a_path) || !file.exists(b_path)) return(NULL)
  a <- utils::read.delim(a_path, sep = "\t", header = TRUE, stringsAsFactors = FALSE, check.names = FALSE)
  b <- utils::read.delim(b_path, sep = "\t", header = TRUE, stringsAsFactors = FALSE, check.names = FALSE)
  req <- c("dataset_id", "barcode", "cell_type", "weight")
  if (!all(req %in% colnames(a)) || !all(req %in% colnames(b))) return(NULL)

  a <- a[a$dataset_id == dataset_id, req, drop = FALSE]
  b <- b[b$dataset_id == dataset_id, req, drop = FALSE]
  if (nrow(a) < 1000 || nrow(b) < 1000) return(NULL)

  if (!is.null(b_strip_prefix) && nzchar(b_strip_prefix)) {
    b$cell_type <- sub(paste0("^", b_strip_prefix), "", b$cell_type)
  }

  common_barcodes <- intersect(unique(a$barcode), unique(b$barcode))
  if (length(common_barcodes) < 200) return(NULL)
  a <- a[a$barcode %in% common_barcodes, , drop = FALSE]
  b <- b[b$barcode %in% common_barcodes, , drop = FALSE]

  common_cell_types <- intersect(unique(a$cell_type), unique(b$cell_type))
  common_cell_types <- common_cell_types[!is.na(common_cell_types) & common_cell_types != ""]
  if (length(common_cell_types) < 5) return(NULL)
  a <- a[a$cell_type %in% common_cell_types, , drop = FALSE]
  b <- b[b$cell_type %in% common_cell_types, , drop = FALSE]

  amat <- stats::xtabs(weight ~ barcode + cell_type, data = a)
  bmat <- stats::xtabs(weight ~ barcode + cell_type, data = b)
  common_cols <- intersect(colnames(amat), colnames(bmat))
  if (length(common_cols) < 5) return(NULL)
  amat <- amat[, common_cols, drop = FALSE]
  bmat <- bmat[, common_cols, drop = FALSE]

  # L1 normalize per spot to make methods comparable.
  rs <- rowSums(amat); rs[rs == 0] <- 1
  ts <- rowSums(bmat); ts[ts == 0] <- 1
  amat <- amat / rs
  bmat <- bmat / ts

  num <- rowSums(amat * bmat)
  den <- sqrt(rowSums(amat * amat)) * sqrt(rowSums(bmat * bmat))
  den[den == 0] <- NA
  cos <- num / den
  cos <- as.numeric(cos[is.finite(cos)])
  if (length(cos) < 200) return(NULL)

  q25 <- as.numeric(stats::quantile(cos, 0.25, names = FALSE))
  q50 <- as.numeric(stats::median(cos))
  q75 <- as.numeric(stats::quantile(cos, 0.75, names = FALSE))
  list(
    a_label = a_label,
    b_label = b_label,
    n_spots = length(cos),
    q25 = q25,
    q50 = q50,
    q75 = q75
  )
}

# Panel A: QC + clustering summary for canonical PBMC datasets
qc_metrics <- c("n_cells_after_qc", "median_n_genes_by_counts", "median_pct_counts_mt", "n_clusters")
pbmc <- sc[sc$dataset_id %in% c("10x_PBMC_3k_scRNA_2016_S3", "10x_PBMC_10k_scRNA") &
             sc$metric_id %in% qc_metrics, ]

pbmc$dataset_short <- ifelse(pbmc$dataset_id == "10x_PBMC_3k_scRNA_2016_S3", "PBMC 3k", "PBMC 10k")
pbmc$metric_label <- factor(pbmc$metric_id,
                            levels = qc_metrics,
                            labels = c("Cells after QC", "Median genes / cell", "Median % mt", "Clusters"))

summarize_iqr <- function(v) {
  v <- v[is.finite(v)]
  if (length(v) == 0) return(c(med = NA, q25 = NA, q75 = NA, n = 0))
  c(med = stats::median(v), q25 = as.numeric(stats::quantile(v, 0.25)), q75 = as.numeric(stats::quantile(v, 0.75)), n = length(v))
}

pbmc_sum <- aggregate(metric_value ~ dataset_short + method_clean + metric_label, data = pbmc, FUN = summarize_iqr)
pbmc_sum$med <- pbmc_sum$metric_value[, "med"]
pbmc_sum$q25 <- pbmc_sum$metric_value[, "q25"]
pbmc_sum$q75 <- pbmc_sum$metric_value[, "q75"]
pbmc_sum$n <- pbmc_sum$metric_value[, "n"]

p_qc <- ggplot(pbmc_sum, aes(x = method_clean, y = med, color = dataset_short)) +
  geom_pointrange(aes(ymin = q25, ymax = q75), position = position_dodge(width = 0.35), linewidth = 0.6) +
  geom_point(position = position_dodge(width = 0.35), size = 2.1) +
  facet_wrap(~metric_label, scales = "free_y", nrow = 1, labeller = label_wrap_gen(width = 10)) +
  labs(
    title = "Cross-method comparability (scRNA baseline modules)",
    subtitle = "Median (point) and IQR (line) across reruns under unified preprocessing",
    x = NULL,
    y = NULL,
    color = NULL
  ) +
  base_theme +
  scale_color_manual(values = c("PBMC 10k" = cloudbio_palette$blue_dark, "PBMC 3k" = cloudbio_palette$blue_light)) +
  theme(axis.text.x = element_text(angle = 30, hjust = 1))

# Panel B: integration comparison (batch mixing + agreement vs baseline)
int_metrics <- c("batch_mixing_nn_frac_mean", "ari_clusters_vs_baseline")
integ <- sc[grepl("INTEGRATION", sc$dataset_id) & sc$metric_id %in% int_metrics, ]
if (nrow(integ) > 0) {
  integ$scenario <- ifelse(grepl("PBMC3K_PLUS_PBMC10K", integ$dataset_id), "PBMC3k+10k", "integration-pair")
  integ$metric_label <- factor(integ$metric_id,
                               levels = int_metrics,
                               labels = c("Batch mixing (NN fraction)", "ARI vs baseline clusters"))
  integ_sum <- aggregate(metric_value ~ scenario + method_clean + metric_label, data = integ, FUN = summarize_iqr)
  integ_sum$med <- integ_sum$metric_value[, "med"]
  integ_sum$q25 <- integ_sum$metric_value[, "q25"]
  integ_sum$q75 <- integ_sum$metric_value[, "q75"]
  integ_sum$n <- integ_sum$metric_value[, "n"]

		  p_int <- ggplot(integ_sum, aes(x = method_clean, y = med, color = scenario)) +
	    geom_hline(yintercept = 0, linewidth = 0.2, color = "grey75") +
	    geom_pointrange(aes(ymin = q25, ymax = q75), position = position_dodge(width = 0.35), linewidth = 0.6) +
	    geom_point(position = position_dodge(width = 0.35), size = 2.1) +
	    facet_wrap(~metric_label, scales = "free_y", ncol = 2) +
		    labs(
		      title = "Cross-method comparability (scRNA integration modules)",
		      subtitle = "Batch-mixing gain and clustering agreement vs baseline under fixed preprocessing",
	      x = NULL,
	      y = NULL,
	      color = NULL
	    ) +
	    base_theme +
	    scale_color_manual(values = c("PBMC3k+10k" = cloudbio_palette$blue_dark, "integration-pair" = cloudbio_palette$blue_light)) +
	    theme(axis.text.x = element_text(angle = 30, hjust = 1))
} else {
  p_int <- ggplot() + geom_blank() + labs(title = "Integration comparison on multi-batch inputs",
                                         subtitle = "No integration rows found in the current anchor table") + base_theme
}

# Panel C: Visium deconvolution summary on a canonical dataset
vis_metrics <- c("mean_entropy", "mean_max_weight", "n_cell_types", "gene_overlap")
vis <- df[df$modality == "Visium" &
            df$dataset_id == dataset_visium &
            df$metric_id %in% vis_metrics &
            !is.na(df$metric_value), ]
if (nrow(vis) > 0) {
  vis$method_clean <- gsub("^baseline:", "", vis$method_id)
  vis$metric_label <- factor(
    vis$metric_id,
    levels = vis_metrics,
    labels = c("Mean entropy (lower = sharper)", "Mean max weight (higher = sharper)", "Predicted cell types", "Gene overlap (ref vs Visium)")
  )
  vis_sum <- aggregate(metric_value ~ method_clean + metric_label, data = vis, FUN = summarize_iqr)
  vis_sum$med <- vis_sum$metric_value[, "med"]
  vis_sum$q25 <- vis_sum$metric_value[, "q25"]
  vis_sum$q75 <- vis_sum$metric_value[, "q75"]
  vis_sum$n <- vis_sum$metric_value[, "n"]

  conc_parts <- list()
  conc_rt <- spotwise_cosine_summary(rctd_tsv, tangram_tsv, dataset_visium, "RCTD", "Tangram")
  if (!is.null(conc_rt)) conc_parts[[length(conc_parts) + 1]] <- conc_rt
  conc_tc <- spotwise_cosine_summary(tangram_tsv, cell2_tsv, dataset_visium, "Tangram", "cell2location", b_strip_prefix = "meanscell_abundance_w_sf_")
  if (!is.null(conc_tc)) conc_parts[[length(conc_parts) + 1]] <- conc_tc

  conc_caption <- NULL
  conc_caption <- concordance_caption(concord_infile, dataset_visium)
  if (is.null(conc_caption) && length(conc_parts) > 0) {
    fmt <- function(x) sprintf("%.2f", x)
    chunks <- vapply(conc_parts, function(s) {
      paste0(
        s$a_label, " vs ", s$b_label, ": median=", fmt(s$q50),
        " (IQR=", fmt(s$q75 - s$q25), ", n_spots=", s$n_spots, ")"
      )
    }, character(1))
    conc_caption <- paste("Cross-method concordance (spotwise cosine similarity; L1-normalized):", paste(chunks, collapse = " | "))
  }

  badge <- concordance_badge(concord_infile, dataset_visium, "rctd_vs_tangram")
  vis_subtitle <- paste0(
    pretty_visium_label(dataset_visium),
    ": unified preprocessing, standardized intermediate tables, and predeclared deconvolution runners"
  )
  if (!is.null(badge)) vis_subtitle <- paste0(vis_subtitle, "\n", badge)
  vis_subtitle <- wrap_text(vis_subtitle, width = 62)

  p_vis <- ggplot(vis_sum, aes(x = method_clean, y = med)) +
    geom_pointrange(aes(ymin = q25, ymax = q75), linewidth = 0.6, color = cloudbio_palette$accent) +
    geom_point(size = 2.1, color = cloudbio_palette$accent) +
    facet_wrap(~metric_label, scales = "free_y", ncol = 1, labeller = label_wrap_gen(width = 18)) +
    scale_y_continuous(n.breaks = 3) +
    labs(
      title = "Cross-method comparability (Visium deconvolution)",
      subtitle = vis_subtitle,
      x = NULL,
      y = NULL,
      caption = conc_caption
    ) +
    base_theme +
    coord_cartesian(clip = "off") +
    theme(
      axis.text.x = element_text(angle = 30, hjust = 1),
      axis.text.y = element_text(size = 8),
      plot.caption = element_text(size = 7.2, color = "#333333", hjust = 0, lineheight = 1.05, margin = margin(t = 6)),
      plot.caption.position = "plot",
      plot.margin = margin(10, 42, 20, 14, "pt")
    )
} else {
  p_vis <- ggplot() + geom_blank() + labs(
    title = "Cross-method comparability (Visium deconvolution)",
    subtitle = "No Visium deconvolution rows found in the current anchor table"
  ) + base_theme
}

final <- (p_qc / p_int) | p_vis
final <- final + patchwork::plot_layout(widths = c(0.98, 1.14))
final <- final + plot_annotation(tag_levels = "A")
final <- final & cloudbio_tag_theme()

ggplot2::ggsave(out_pdf, plot = final, width = 11.69, height = 8.27, units = "in", device = "pdf")
ggplot2::ggsave(out_png, plot = final, width = 11.69, height = 8.27, units = "in", dpi = 300)

cat(paste("Wrote:", out_pdf, "\n"))
cat(paste("Wrote:", out_png, "\n"))
