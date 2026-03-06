#!/usr/bin/env Rscript
args <- commandArgs(trailingOnly = TRUE)

infile <- "results/benchmarks/method_benchmark.tsv"
outdir <- "plots/publication"
dataset_id <- "Mouse_Brain_Visium_10x"
k_top <- 3
max_spots_plot <- 5000

spots_tsv <- "results/figures/visium_spots.tsv"
rctd_tsv <- "results/figures/visium_celltype_weights_rctd.tsv"
tangram_tsv <- "results/figures/visium_celltype_weights_tangram.tsv"
cell2_tsv <- "results/figures/visium_celltype_weights_cell2location.tsv"

for (a in args) {
  if (startsWith(a, "--infile=")) infile <- sub("^--infile=", "", a)
  if (startsWith(a, "--outdir=")) outdir <- sub("^--outdir=", "", a)
  if (startsWith(a, "--dataset-id=")) dataset_id <- sub("^--dataset-id=", "", a)
  if (startsWith(a, "--k=")) k_top <- as.integer(sub("^--k=", "", a))
  if (startsWith(a, "--max-spots=")) max_spots_plot <- as.integer(sub("^--max-spots=", "", a))
  if (startsWith(a, "--spots=")) spots_tsv <- sub("^--spots=", "", a)
  if (startsWith(a, "--rctd=")) rctd_tsv <- sub("^--rctd=", "", a)
  if (startsWith(a, "--tangram=")) tangram_tsv <- sub("^--tangram=", "", a)
  if (startsWith(a, "--cell2location=")) cell2_tsv <- sub("^--cell2location=", "", a)
}

out_pdf <- file.path(outdir, "pdf", "F4_spatial_benchmark.pdf")
out_png <- file.path(outdir, "png", "F4_spatial_benchmark.png")

dir.create(dirname(out_pdf), recursive = TRUE, showWarnings = FALSE)
dir.create(dirname(out_png), recursive = TRUE, showWarnings = FALSE)

placeholder <- function(reason) {
  n_rows <- 0
  if (file.exists(infile)) {
    lines <- readLines(infile, warn = FALSE)
    n_rows <- max(length(lines) - 1, 0)
  }

  pdf(out_pdf, width = 8.27, height = 11.69, useDingbats = FALSE)
  plot.new()
  text(
    0, 1,
    adj = c(0, 1),
    labels = paste(
      "F4: Interoperability fidelity in multi-modal spatial mapping (fallback placeholder)",
      "",
      paste("Dataset:", dataset_id),
      paste("Reason:", reason),
      "",
      paste("Benchmark table:", infile),
      paste("Rows:", n_rows),
      "",
      "To enable spatial deconvolution maps, ensure these exist:",
      paste0("- ", spots_tsv),
      paste0("- ", rctd_tsv),
      paste0("- ", tangram_tsv),
      paste0("- ", cell2_tsv, " (optional)"),
      sep = "\n"
    )
  )
  dev.off()

  png(out_png, width = 2480, height = 3508, res = 300)
  plot.new()
  text(
    0, 1,
    adj = c(0, 1),
    labels = paste(
      "F4: Interoperability fidelity in multi-modal spatial mapping (fallback placeholder)",
      "",
      paste("Dataset:", dataset_id),
      paste("Reason:", reason),
      "",
      paste("Benchmark table:", infile),
      paste("Rows:", n_rows),
      "",
      "To enable spatial deconvolution maps, ensure these exist:",
      paste0("- ", spots_tsv),
      paste0("- ", rctd_tsv),
      paste0("- ", tangram_tsv),
      paste0("- ", cell2_tsv, " (optional)"),
      sep = "\n"
    )
  )
  dev.off()

  cat(paste("Wrote:", out_pdf, "\n"))
  cat(paste("Wrote:", out_png, "\n"))
}

if (!file.exists(infile)) {
  placeholder(paste("Missing benchmark table:", infile))
  quit(status = 0)
}

if (!file.exists(spots_tsv) || !file.exists(rctd_tsv) || !file.exists(tangram_tsv)) {
  placeholder("Missing one or more Visium deconvolution anchor tables.")
  quit(status = 0)
}

if (!requireNamespace("ggplot2", quietly = TRUE) || !requireNamespace("patchwork", quietly = TRUE)) {
  placeholder("Missing R plotting deps (need ggplot2 + patchwork).")
  quit(status = 0)
}

library(ggplot2)
library(patchwork)
this_file <- NULL
for (a in commandArgs(trailingOnly = FALSE)) if (startsWith(a, "--file=")) this_file <- sub("^--file=", "", a)
this_dir <- if (is.null(this_file)) getwd() else dirname(normalizePath(this_file))
source(file.path(this_dir, "_cloudbio_theme.R"))

read_tsv <- function(path) {
  utils::read.delim(path, sep = "\t", header = TRUE, stringsAsFactors = FALSE, check.names = FALSE)
}

pretty_visium_label <- function(x) {
  if (is.na(x) || !nzchar(x)) return("Visium dataset")
  if (x == "Mouse_Brain_Visium_10x") return("Mouse brain Visium")
  if (grepl("lymph", x, ignore.case = TRUE)) return("Human lymph node Visium")
  gsub("_", " ", gsub("^10x_", "", x))
}

spots <- read_tsv(spots_tsv)
if (!("dataset_id" %in% colnames(spots)) || !("barcode" %in% colnames(spots))) {
  placeholder(paste("Invalid spots table (missing dataset_id/barcode):", spots_tsv))
  quit(status = 0)
}
spots <- spots[spots$dataset_id == dataset_id, , drop = FALSE]
if (nrow(spots) < 50) {
  placeholder(paste("Too few spots for dataset_id=", dataset_id, " in ", spots_tsv))
  quit(status = 0)
}

keep_cols <- intersect(colnames(spots), c("barcode", "array_row", "array_col", "pxl_row_in_fullres", "pxl_col_in_fullres"))
spots <- spots[, keep_cols, drop = FALSE]

weights_rctd <- read_tsv(rctd_tsv)
weights_tang <- read_tsv(tangram_tsv)
weights_cell2 <- NULL
if (file.exists(cell2_tsv)) {
  weights_cell2 <- read_tsv(cell2_tsv)
}

for (nm in c("dataset_id", "barcode", "cell_type", "weight")) {
  if (!(nm %in% colnames(weights_rctd)) || !(nm %in% colnames(weights_tang))) {
    placeholder(paste("Invalid weight tables (missing columns):", nm))
    quit(status = 0)
  }
  if (!is.null(weights_cell2) && !(nm %in% colnames(weights_cell2))) {
    placeholder(paste("Invalid cell2location weight table (missing columns):", nm))
    quit(status = 0)
  }
}

weights_rctd <- weights_rctd[weights_rctd$dataset_id == dataset_id, , drop = FALSE]
weights_tang <- weights_tang[weights_tang$dataset_id == dataset_id, , drop = FALSE]
if (!is.null(weights_cell2)) {
  weights_cell2 <- weights_cell2[weights_cell2$dataset_id == dataset_id, , drop = FALSE]
  if (nrow(weights_cell2) < 1000) {
    weights_cell2 <- NULL
  }
}

if (nrow(weights_rctd) < 1000 || nrow(weights_tang) < 1000) {
  placeholder("Too few deconvolution rows for the requested dataset (expected spot×cell_type long tables).")
  quit(status = 0)
}

if (!is.null(weights_cell2)) {
  # Align cell2location cell-type naming with other methods.
  weights_cell2$cell_type <- sub("^meanscell_abundance_w_sf_", "", weights_cell2$cell_type)
}

spots_barcodes <- unique(spots$barcode)
weights_rctd <- weights_rctd[weights_rctd$barcode %in% spots_barcodes, , drop = FALSE]
weights_tang <- weights_tang[weights_tang$barcode %in% spots_barcodes, , drop = FALSE]
if (!is.null(weights_cell2)) {
  weights_cell2 <- weights_cell2[weights_cell2$barcode %in% spots_barcodes, , drop = FALSE]
}

if (!("array_row" %in% colnames(spots)) || !("array_col" %in% colnames(spots))) {
  placeholder(paste("Invalid spots table (missing array_row/array_col):", spots_tsv))
  quit(status = 0)
}

if (nrow(spots) > max_spots_plot && max_spots_plot > 0) {
  set.seed(0)
  idx <- sample.int(nrow(spots), size = max_spots_plot, replace = FALSE)
  spots <- spots[idx, , drop = FALSE]
  spots_barcodes <- unique(spots$barcode)
  weights_rctd <- weights_rctd[weights_rctd$barcode %in% spots_barcodes, , drop = FALSE]
  weights_tang <- weights_tang[weights_tang$barcode %in% spots_barcodes, , drop = FALSE]
  if (!is.null(weights_cell2)) {
    weights_cell2 <- weights_cell2[weights_cell2$barcode %in% spots_barcodes, , drop = FALSE]
  }
}

normalize_weights <- function(df) {
  df <- df[, c("barcode", "cell_type", "weight"), drop = FALSE]
  df$weight <- suppressWarnings(as.numeric(df$weight))
  df$weight[!is.finite(df$weight) | df$weight < 0] <- 0
  s <- stats::ave(df$weight, df$barcode, FUN = sum)
  s[s == 0] <- 1
  df$w_norm <- df$weight / s
  df
}

weights_rctd_n <- normalize_weights(weights_rctd)
weights_tang_n <- normalize_weights(weights_tang)
weights_cell2_n <- NULL
if (!is.null(weights_cell2)) weights_cell2_n <- normalize_weights(weights_cell2)

common_ct <- intersect(unique(weights_rctd_n$cell_type), unique(weights_tang_n$cell_type))
if (!is.null(weights_cell2_n)) common_ct <- intersect(common_ct, unique(weights_cell2_n$cell_type))
common_ct <- common_ct[!is.na(common_ct) & common_ct != ""]
if (length(common_ct) < 5) {
  placeholder("Too few shared cell types across deconvolution methods (unexpected).")
  quit(status = 0)
}

mean_by_ct <- function(df_norm, cts) {
  df_norm <- df_norm[df_norm$cell_type %in% cts, , drop = FALSE]
  out <- stats::aggregate(w_norm ~ cell_type, df_norm, mean)
  colnames(out) <- c("cell_type", "mean_weight")
  out
}

means <- list(
  RCTD = mean_by_ct(weights_rctd_n, common_ct),
  Tangram = mean_by_ct(weights_tang_n, common_ct)
)
if (!is.null(weights_cell2_n)) means$`cell2location` <- mean_by_ct(weights_cell2_n, common_ct)
mt <- Reduce(function(x, y) merge(x, y, by = "cell_type", all = TRUE), lapply(names(means), function(nm) {
  d <- means[[nm]]
  colnames(d) <- c("cell_type", paste0("mean_", nm))
  d
}))
for (nm in names(means)) {
  col <- paste0("mean_", nm)
  mt[[col]][is.na(mt[[col]])] <- 0
}
mean_cols <- grep("^mean_", colnames(mt), value = TRUE)
mt$mean_all <- rowMeans(mt[, mean_cols, drop = FALSE])
mt <- mt[order(-mt$mean_all, mt$cell_type), , drop = FALSE]
if (is.na(k_top) || k_top < 1) k_top <- 3
cell_types <- head(mt$cell_type, k_top)
cell_types <- cell_types[!is.na(cell_types) & cell_types != ""]
if (length(cell_types) < 1) {
  placeholder("No cell types available after filtering (unexpected).")
  quit(status = 0)
}

plot_map_theme <- theme_minimal(base_family = "sans", base_size = 9) +
  theme(
    plot.title = element_text(size = 11, face = "bold"),
    plot.subtitle = element_text(size = 9),
    axis.title = element_blank(),
    axis.text = element_blank(),
    axis.ticks = element_blank(),
    panel.grid = element_blank(),
    strip.text = element_text(size = 9, face = "bold"),
    legend.title = element_text(size = 8),
    legend.text = element_text(size = 8),
    plot.margin = margin(10, 18, 14, 18, "pt")
  )

build_map_df <- function(method_label, df_norm) {
  df_norm <- df_norm[df_norm$cell_type %in% cell_types, , drop = FALSE]
  df_norm <- df_norm[, c("barcode", "cell_type", "w_norm"), drop = FALSE]
  colnames(df_norm) <- c("barcode", "cell_type", "weight")
  df_norm$method <- method_label
  m <- merge(spots, df_norm, by = "barcode", all.x = TRUE)
  m$weight[is.na(m$weight)] <- 0
  m$cell_type <- factor(m$cell_type, levels = cell_types)
  m$method <- factor(m$method, levels = c("RCTD", "Tangram", if (!is.null(weights_cell2_n)) "cell2location" else NULL))
  m
}

maps <- build_map_df("RCTD", weights_rctd_n)
maps <- rbind(maps, build_map_df("Tangram", weights_tang_n))
if (!is.null(weights_cell2_n)) maps <- rbind(maps, build_map_df("cell2location", weights_cell2_n))

hi <- as.numeric(stats::quantile(maps$weight, 0.98, na.rm = TRUE))
if (!is.finite(hi) || hi <= 0) hi <- 1

p_maps <- ggplot(maps, aes(x = array_col, y = array_row, color = weight)) +
  geom_point(size = 0.55, alpha = 0.95) +
  scale_y_reverse() +
  coord_equal() +
  facet_grid(cell_type ~ method) +
  scale_color_gradientn(
    colors = c("#f7fbff", "#c6dbef", "#6baed6", "#2171b5", "#08306b"),
    limits = c(0, hi),
    oob = scales::squish,
    trans = "sqrt"
  ) +
  labs(
    title = "Interoperability fidelity in multi-modal spatial mapping",
    subtitle = paste0(
      pretty_visium_label(dataset_id),
      " (n=", format(nrow(spots), big.mark = ","), " spots); ",
      "L1-normalized spot compositions from standardized intermediate tables"
    ),
    color = "Weight"
  ) +
  plot_map_theme

derive_uncertainty <- function(df_norm) {
  df_norm <- df_norm[, c("barcode", "cell_type", "w_norm"), drop = FALSE]
  p <- df_norm$w_norm
  p[p < 1e-12] <- 1e-12
  ent <- tapply(p, df_norm$barcode, FUN = function(x) -sum(x * log(x)))
  mx <- tapply(df_norm$w_norm, df_norm$barcode, FUN = function(x) max(x))
  data.frame(barcode = names(ent), entropy = as.numeric(ent), max_weight = as.numeric(mx), stringsAsFactors = FALSE)
}

u_r <- derive_uncertainty(weights_rctd_n)
u_t <- derive_uncertainty(weights_tang_n)
u_c <- NULL
if (!is.null(weights_cell2_n)) u_c <- derive_uncertainty(weights_cell2_n)

uncert <- list(
  transform(merge(spots, u_r, by = "barcode", all.x = TRUE), method = "RCTD"),
  transform(merge(spots, u_t, by = "barcode", all.x = TRUE), method = "Tangram")
)
if (!is.null(u_c)) uncert <- c(uncert, list(transform(merge(spots, u_c, by = "barcode", all.x = TRUE), method = "cell2location")))
uncert_df <- do.call(rbind, uncert)
uncert_df$entropy[is.na(uncert_df$entropy)] <- 0
uncert_df$max_weight[is.na(uncert_df$max_weight)] <- 0

uncert_long <- rbind(
  transform(uncert_df[, c("array_col", "array_row", "method"), drop = FALSE], metric = "Entropy", value = uncert_df$entropy),
  transform(uncert_df[, c("array_col", "array_row", "method"), drop = FALSE], metric = "Max weight", value = uncert_df$max_weight)
)
uncert_long$method <- factor(uncert_long$method, levels = c("RCTD", "Tangram", if (!is.null(u_c)) "cell2location" else NULL))
uncert_long$metric <- factor(uncert_long$metric, levels = c("Entropy", "Max weight"))

u_hi <- as.numeric(stats::quantile(uncert_long$value, 0.99, na.rm = TRUE))
if (!is.finite(u_hi) || u_hi <= 0) u_hi <- max(uncert_long$value, na.rm = TRUE)

p_uncert <- ggplot(uncert_long, aes(x = array_col, y = array_row, color = value)) +
  geom_point(size = 0.45, alpha = 0.95) +
  scale_y_reverse() +
  coord_equal() +
  facet_grid(method ~ metric) +
  scale_color_gradientn(
    colors = c("#fff5f0", "#fcbba1", "#fb6a4a", "#cb181d", "#67000d"),
    limits = c(0, u_hi),
    oob = scales::squish,
    trans = "sqrt"
  ) +
  labs(
    title = "Uncertainty proxies for spatial mapping fidelity",
    subtitle = "Entropy (higher = more mixed) and max weight (higher = sharper)",
    color = NULL
  ) +
  plot_map_theme

spotwise_cosine <- function(a_norm, b_norm, cts) {
  a <- a_norm[a_norm$cell_type %in% cts, , drop = FALSE]
  b <- b_norm[b_norm$cell_type %in% cts, , drop = FALSE]
  amat <- stats::xtabs(w_norm ~ barcode + cell_type, data = a)
  bmat <- stats::xtabs(w_norm ~ barcode + cell_type, data = b)
  common_cols <- intersect(colnames(amat), colnames(bmat))
  if (length(common_cols) < 5) return(NULL)
  amat <- amat[, common_cols, drop = FALSE]
  bmat <- bmat[, common_cols, drop = FALSE]
  common_rows <- intersect(rownames(amat), rownames(bmat))
  if (length(common_rows) < 200) return(NULL)
  amat <- amat[common_rows, , drop = FALSE]
  bmat <- bmat[common_rows, , drop = FALSE]
  num <- rowSums(amat * bmat)
  den <- sqrt(rowSums(amat * amat)) * sqrt(rowSums(bmat * bmat))
  den[den == 0] <- NA
  cos <- num / den
  cos <- as.numeric(cos[is.finite(cos)])
  cos
}

conc_rows <- list()
cos_rt <- spotwise_cosine(weights_rctd_n, weights_tang_n, common_ct)
if (!is.null(cos_rt)) conc_rows[[length(conc_rows) + 1]] <- data.frame(pair = "RCTD vs Tangram", cosine = cos_rt)
if (!is.null(weights_cell2_n)) {
  cos_tc <- spotwise_cosine(weights_tang_n, weights_cell2_n, common_ct)
  if (!is.null(cos_tc)) conc_rows[[length(conc_rows) + 1]] <- data.frame(pair = "Tangram vs cell2location", cosine = cos_tc)
}
conc_df <- do.call(rbind, conc_rows)

if (!is.null(conc_df) && nrow(conc_df) > 0) {
  conc_sum <- aggregate(cosine ~ pair, conc_df, function(v) c(med = stats::median(v), q25 = as.numeric(stats::quantile(v, 0.25)), q75 = as.numeric(stats::quantile(v, 0.75)), n = length(v)))
  conc_sum$med <- conc_sum$cosine[, "med"]
  conc_sum$q25 <- conc_sum$cosine[, "q25"]
  conc_sum$q75 <- conc_sum$cosine[, "q75"]
  conc_sum$n <- conc_sum$cosine[, "n"]

  p_conc <- ggplot(conc_sum, aes(x = med, y = reorder(pair, med))) +
    geom_vline(xintercept = 0.5, linewidth = 0.35, linetype = "dashed", color = "grey70") +
    geom_pointrange(aes(xmin = q25, xmax = q75), linewidth = 0.55, color = cloudbio_palette$accent) +
    geom_point(size = 2.2, color = cloudbio_palette$accent) +
    geom_text(aes(label = paste0("median=", sprintf("%.2f", med), "; n=", n)), hjust = -0.10, size = 3.0, color = "#333333") +
    scale_x_continuous(limits = c(0, 1), expand = expansion(mult = c(0, 0.20))) +
    labs(
      title = "Cross-method concordance (spotwise cosine)",
      subtitle = "Across RCTD/Tangram/cell2location where available (median and IQR); dashed line at 0.5",
      x = "Cosine similarity (higher = more similar)",
      y = NULL
    ) +
    cloudbio_base_theme() +
    coord_cartesian(clip = "off")
} else {
  p_conc <- ggplot() + geom_blank() + labs(title = "Cross-method concordance", subtitle = "No concordance data available") + cloudbio_base_theme()
}

right <- p_uncert / p_conc
final <- p_maps | right
final <- final + plot_annotation(tag_levels = "A")
final <- final & cloudbio_tag_theme()

ggplot2::ggsave(out_pdf, plot = final, width = 11.69, height = 8.27, units = "in", device = "pdf")
ggplot2::ggsave(out_png, plot = final, width = 11.69, height = 8.27, units = "in", dpi = 300)

cat(paste("Wrote:", out_pdf, "\n"))
cat(paste("Wrote:", out_png, "\n"))
