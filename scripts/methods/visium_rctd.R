#!/usr/bin/env Rscript

args <- commandArgs(trailingOnly = TRUE)

get_arg <- function(name, default = NULL) {
  prefix <- paste0("--", name, "=")
  for (a in args) {
    if (startsWith(a, prefix)) return(sub(prefix, "", a))
  }
  return(default)
}

visium_dir <- get_arg("visium-dir", NULL)
scrna_dir <- get_arg("scrna-dir", NULL)
labels_tsv <- get_arg("labels-tsv", NULL)
dataset_id <- get_arg("dataset-id", "")
reference_dataset_id <- get_arg("reference-dataset-id", "")
out_weights_tsv <- get_arg("out-weights-tsv", NULL)
out_json <- get_arg("out-json", NULL)
seed <- as.integer(get_arg("seed", "1"))

if (is.null(visium_dir) || is.null(scrna_dir) || is.null(labels_tsv) || is.null(out_weights_tsv) || is.null(out_json)) {
  stop(
    "Usage: visium_rctd.R ",
    "--visium-dir=PATH --scrna-dir=PATH --labels-tsv=PATH ",
    "--out-weights-tsv=PATH --out-json=PATH [--dataset-id=ID] [--reference-dataset-id=ID] [--seed=1]"
  )
}

if (!dir.exists(visium_dir)) stop(paste("visium-dir not found:", visium_dir))
if (!dir.exists(scrna_dir)) stop(paste("scrna-dir not found:", scrna_dir))
if (!file.exists(labels_tsv)) stop(paste("labels-tsv not found:", labels_tsv))

if (!requireNamespace("spacexr", quietly = TRUE)) {
  stop("Missing R package 'spacexr' (RCTD). Install it in the container image (pinned) before running.")
}
if (!requireNamespace("Matrix", quietly = TRUE)) stop("Missing R package 'Matrix'.")
if (!requireNamespace("jsonlite", quietly = TRUE)) stop("Missing R package 'jsonlite'.")

options(bspm.sudo = TRUE)
if (is.na(seed) || seed < 1) seed <- 1
set.seed(seed)

pick <- function(dir, names) {
  for (n in names) {
    p <- file.path(dir, n)
    if (file.exists(p)) return(p)
  }
  stop(paste0("Missing required 10x file in ", dir, ": expected one of {", paste(names, collapse = ", "), "}"))
}

open_text <- function(p) {
  if (endsWith(p, ".gz")) return(gzfile(p, open = "rt"))
  return(file(p, open = "rt"))
}
open_bin <- function(p) {
  if (endsWith(p, ".gz")) return(gzfile(p, open = "rb"))
  return(file(p, open = "rb"))
}

read_10x_any <- function(dir) {
  mtx <- pick(dir, c("matrix.mtx.gz", "matrix.mtx"))
  feats <- pick(dir, c("features.tsv.gz", "features.tsv", "genes.tsv.gz", "genes.tsv"))
  bcs <- pick(dir, c("barcodes.tsv.gz", "barcodes.tsv"))

  con <- open_bin(mtx)
  on.exit(close(con), add = TRUE)
  mat <- Matrix::readMM(con)
  if (!inherits(mat, "dgCMatrix")) mat <- as(mat, "CsparseMatrix")
  if (!inherits(mat, "dgCMatrix")) mat <- as(mat, "dgCMatrix")

  feats_con <- open_text(feats)
  on.exit(close(feats_con), add = TRUE)
  feats_df <- utils::read.table(feats_con, sep = "\t", header = FALSE, stringsAsFactors = FALSE, quote = "", comment.char = "")
  if (ncol(feats_df) == 1) {
    gene_symbols <- feats_df[[1]]
  } else {
    gene_symbols <- feats_df[[2]]
  }

  bcs_con <- open_text(bcs)
  on.exit(close(bcs_con), add = TRUE)
  barcodes <- readLines(bcs_con, warn = FALSE)

  if (nrow(mat) != length(gene_symbols)) stop("10x features mismatch: matrix rows != features.tsv rows")
  if (ncol(mat) != length(barcodes)) stop("10x barcodes mismatch: matrix cols != barcodes.tsv rows")

  rownames(mat) <- make.unique(gene_symbols)
  colnames(mat) <- barcodes
  return(mat)
}

read_visium_positions <- function(spatial_dir) {
  pos_csv <- file.path(spatial_dir, "tissue_positions.csv")
  pos_list <- file.path(spatial_dir, "tissue_positions_list.csv")
  if (file.exists(pos_csv)) {
    df <- utils::read.csv(pos_csv, stringsAsFactors = FALSE)
  } else if (file.exists(pos_list)) {
    df <- utils::read.csv(pos_list, header = FALSE, stringsAsFactors = FALSE)
    colnames(df) <- c("barcode", "in_tissue", "array_row", "array_col", "pxl_row_in_fullres", "pxl_col_in_fullres")
  } else {
    stop(paste0("missing tissue positions file under ", spatial_dir))
  }
  # normalize
  if (!("barcode" %in% colnames(df))) stop("tissue positions missing barcode column")
  if (!("in_tissue" %in% colnames(df))) stop("tissue positions missing in_tissue column")
  df$barcode <- as.character(df$barcode)
  df$in_tissue <- as.integer(df$in_tissue)
  df$array_row <- as.integer(df$array_row)
  df$array_col <- as.integer(df$array_col)
  return(df)
}

labels_df <- utils::read.table(labels_tsv, sep = "\t", header = TRUE, stringsAsFactors = FALSE, quote = "", comment.char = "")
if (!("barcode" %in% colnames(labels_df)) || !("label" %in% colnames(labels_df))) {
  stop("labels-tsv must have header columns: barcode, label")
}
labels_df$barcode <- as.character(labels_df$barcode)
labels_df$label <- as.character(labels_df$label)

scrna_counts <- read_10x_any(scrna_dir)
common_cells <- intersect(colnames(scrna_counts), labels_df$barcode)
if (length(common_cells) < 50) stop("too few overlapping scRNA barcodes between matrix and labels (need >=50)")
scrna_counts <- scrna_counts[, common_cells, drop = FALSE]
labels_df <- labels_df[match(common_cells, labels_df$barcode), , drop = FALSE]

# RCTD requires >=25 cells per cell type; drop rare types in the reference.
tab_ct <- table(labels_df$label)
keep_ct <- names(tab_ct)[as.integer(tab_ct) >= 25]
mask <- labels_df$label %in% keep_ct
if (sum(mask) < 100) stop("too few reference cells after dropping rare cell types (<100)")
labels_df <- labels_df[mask, , drop = FALSE]
scrna_counts <- scrna_counts[, labels_df$barcode, drop = FALSE]

cell_types <- as.factor(labels_df$label)
names(cell_types) <- labels_df$barcode
nUMI <- Matrix::colSums(scrna_counts)

visium_counts <- read_10x_any(file.path(visium_dir, "filtered_feature_bc_matrix"))
pos <- read_visium_positions(file.path(visium_dir, "spatial"))
pos <- pos[pos$in_tissue == 1, , drop = FALSE]
if (nrow(pos) < 50) stop("too few in-tissue spots (need >=50)")

spots <- intersect(colnames(visium_counts), pos$barcode)
if (length(spots) < 50) stop("too few overlapping Visium barcodes between matrix and spatial positions (need >=50)")
visium_counts <- visium_counts[, spots, drop = FALSE]
pos <- pos[match(spots, pos$barcode), , drop = FALSE]

coords <- data.frame(x = pos$array_col, y = pos$array_row)
rownames(coords) <- pos$barcode

# Gene intersection
genes <- intersect(rownames(scrna_counts), rownames(visium_counts))
if (length(genes) < 1000) stop(paste0("low gene overlap between scRNA and Visium: ", length(genes), " (<1000)"))
scrna_counts <- scrna_counts[genes, , drop = FALSE]
visium_counts <- visium_counts[genes, , drop = FALSE]

ref <- spacexr::Reference(scrna_counts, cell_types, nUMI)
spatialRNA <- spacexr::SpatialRNA(coords, visium_counts)

t0 <- proc.time()[["elapsed"]]
rctd <- spacexr::create.RCTD(spatialRNA, ref, max_cores = 1)
rctd <- spacexr::run.RCTD(rctd, doublet_mode = "full")
wall <- proc.time()[["elapsed"]] - t0

weights <- rctd@results$weights
if (is.null(weights)) stop("RCTD returned no weights")

# Long table: one row per (spot, cell_type)
weights_df <- as.data.frame(as.matrix(weights))
weights_df$barcode <- rownames(weights_df)

cell_types_out <- setdiff(colnames(weights_df), "barcode")
out_long <- data.frame(
  dataset_id = rep(dataset_id, length(cell_types_out) * nrow(weights_df)),
  reference_dataset_id = rep(reference_dataset_id, length(cell_types_out) * nrow(weights_df)),
  barcode = rep(weights_df$barcode, times = length(cell_types_out)),
  cell_type = rep(cell_types_out, each = nrow(weights_df)),
  weight = as.numeric(unlist(weights_df[, cell_types_out, drop = FALSE])),
  stringsAsFactors = FALSE
)

dir.create(dirname(out_weights_tsv), recursive = TRUE, showWarnings = FALSE)
utils::write.table(out_long, out_weights_tsv, sep = "\t", quote = FALSE, row.names = FALSE)

# Simple quality proxies
mat_w <- as.matrix(weights)
row_sums <- rowSums(mat_w)
row_sums[row_sums == 0] <- 1
P <- mat_w / row_sums
entropy <- -rowSums(P * log(pmax(P, 1e-12)))
max_w <- apply(P, 1, max)

metrics <- list(
  n_spots_in_tissue = as.integer(nrow(mat_w)),
  n_cell_types = as.integer(ncol(mat_w)),
  gene_overlap = as.integer(length(genes)),
  mean_entropy = as.numeric(mean(entropy)),
  mean_max_weight = as.numeric(mean(max_w)),
  wall_time_s = as.numeric(wall)
)

versions <- list(
  spacexr = as.character(utils::packageVersion("spacexr")),
  r = as.character(getRversion())
)

out <- list(
  metrics = metrics,
  versions = versions,
  notes = paste0("doublet_mode=full;max_cores=1;seed=", seed)
)

jsonlite::write_json(out, out_json, auto_unbox = TRUE, pretty = TRUE)
cat(paste0("OK: wrote ", out_weights_tsv, " and ", out_json, "\n"))
