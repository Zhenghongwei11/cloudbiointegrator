#!/usr/bin/env Rscript

args <- commandArgs(trailingOnly = TRUE)

get_arg <- function(name, default = NULL) {
  prefix <- paste0("--", name, "=")
  for (a in args) {
    if (startsWith(a, prefix)) return(sub(prefix, "", a))
  }
  return(default)
}

input_dir <- get_arg("input-dir", NULL)
out_json <- get_arg("out-json", NULL)
seed <- as.integer(get_arg("seed", "0"))

if (is.null(input_dir) || is.null(out_json)) {
  stop("Usage: scrna_seurat_v5.R --input-dir=PATH --out-json=PATH [--seed=0]")
}

if (!dir.exists(input_dir)) stop(paste("input-dir not found:", input_dir))

if (!requireNamespace("Seurat", quietly = TRUE)) {
  stop("Missing R package 'Seurat'. Build/run with the Seurat-enabled container environment.")
}
if (!requireNamespace("jsonlite", quietly = TRUE)) {
  stop("Missing R package 'jsonlite'.")
}

# r2u images use bspm; silence harmless container warnings if present.
options(bspm.sudo = TRUE)

# Determinism controls
seed_eff <- seed
if (is.na(seed_eff) || seed_eff < 1) seed_eff <- 1
set.seed(seed_eff)

read_10x_any <- function(dir) {
  pick <- function(names) {
    for (n in names) {
      p <- file.path(dir, n)
      if (file.exists(p)) return(p)
    }
    stop(paste0("Missing required 10x file in ", dir, ": expected one of {", paste(names, collapse = ", "), "}"))
  }

  mtx <- pick(c("matrix.mtx.gz", "matrix.mtx"))
  feats <- pick(c("features.tsv.gz", "features.tsv", "genes.tsv.gz", "genes.tsv"))
  bcs <- pick(c("barcodes.tsv.gz", "barcodes.tsv"))

  open_text <- function(p) {
    if (endsWith(p, ".gz")) return(gzfile(p, open = "rt"))
    return(file(p, open = "rt"))
  }
  open_bin <- function(p) {
    if (endsWith(p, ".gz")) return(gzfile(p, open = "rb"))
    return(file(p, open = "rb"))
  }

  if (!requireNamespace("Matrix", quietly = TRUE)) stop("Missing R package 'Matrix'.")

  con <- open_bin(mtx)
  on.exit(close(con), add = TRUE)
  mat <- Matrix::readMM(con)
  if (!inherits(mat, "dgCMatrix")) mat <- as(mat, "CsparseMatrix")
  if (!inherits(mat, "dgCMatrix")) mat <- as(mat, "dgCMatrix")

  feats_con <- open_text(feats)
  on.exit(close(feats_con), add = TRUE)
  feats_df <- utils::read.table(feats_con, sep = "\t", header = FALSE, stringsAsFactors = FALSE, quote = "", comment.char = "")
  if (ncol(feats_df) == 1) {
    gene_ids <- feats_df[[1]]
    gene_symbols <- feats_df[[1]]
  } else {
    gene_ids <- feats_df[[1]]
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

ari <- function(x, y) {
  x <- as.integer(factor(x))
  y <- as.integer(factor(y))
  tab <- table(x, y)
  n <- sum(tab)
  if (n <= 1) return(NA_real_)
  sum_comb <- function(v) sum(v * (v - 1) / 2)
  sum_ij <- sum_comb(as.vector(tab))
  sum_i <- sum_comb(rowSums(tab))
  sum_j <- sum_comb(colSums(tab))
  expected <- (sum_i * sum_j) / (n * (n - 1) / 2)
  maxv <- 0.5 * (sum_i + sum_j)
  denom <- (maxv - expected)
  if (denom == 0) return(NA_real_)
  return((sum_ij - expected) / denom)
}

counts <- read_10x_any(input_dir)

obj <- Seurat::CreateSeuratObject(counts = counts, project = "scrna", min.cells = 3, min.features = 0)
obj[["percent.mt"]] <- Seurat::PercentageFeatureSet(obj, pattern = "^MT-")

min_features <- 200
max_pct_mt <- 20

obj <- subset(obj, subset = nFeature_RNA >= min_features & percent.mt <= max_pct_mt)

obj <- Seurat::NormalizeData(obj, normalization.method = "LogNormalize", scale.factor = 10000, verbose = FALSE)
obj <- Seurat::FindVariableFeatures(obj, selection.method = "vst", nfeatures = 2000, verbose = FALSE)
obj <- Seurat::ScaleData(obj, features = rownames(obj), verbose = FALSE)
obj <- Seurat::RunPCA(obj, features = Seurat::VariableFeatures(obj), npcs = 30, verbose = FALSE, seed.use = seed_eff)

# Use an exact NN method for better determinism on small datasets.
obj <- Seurat::FindNeighbors(obj, dims = 1:30, nn.method = "rann", verbose = FALSE)
obj <- Seurat::FindClusters(obj, resolution = 0.5, algorithm = 4, random.seed = seed_eff, verbose = FALSE)
obj <- Seurat::RunUMAP(obj, dims = 1:30, umap.method = "uwot", seed.use = seed_eff, verbose = FALSE)

clusters1 <- as.character(Seurat::Idents(obj))

# Robustness: re-run clustering under seed+1 using the same neighbor graph.
set.seed(seed_eff + 1)
obj2 <- obj
obj2 <- Seurat::FindClusters(obj2, resolution = 0.5, algorithm = 4, random.seed = seed_eff + 1, verbose = FALSE)
clusters2 <- as.character(Seurat::Idents(obj2))
ari_seed <- ari(clusters1, clusters2)

md <- obj@meta.data

metrics <- list(
  n_cells_after_qc = as.integer(ncol(obj)),
  n_genes_after_filter = as.integer(nrow(obj)),
  median_total_counts = as.numeric(stats::median(md$nCount_RNA)),
  median_n_genes_by_counts = as.numeric(stats::median(md$nFeature_RNA)),
  median_pct_counts_mt = as.numeric(stats::median(md$percent.mt)),
  n_clusters = as.integer(length(unique(clusters1))),
  qc_min_genes = as.integer(min_features),
  qc_max_pct_mt = as.numeric(max_pct_mt)
)

concordance <- list(
  ari_cluster_seed_plus_1 = as.numeric(ari_seed)
)

versions <- list(
  seurat = as.character(utils::packageVersion("Seurat")),
  seuratobject = if (requireNamespace("SeuratObject", quietly = TRUE)) as.character(utils::packageVersion("SeuratObject")) else "",
  r = as.character(getRversion())
)

out <- list(
  metrics = metrics,
  concordance = concordance,
  versions = versions,
  notes = paste0(
    "seed=", seed_eff,
    "qc_min_features=", min_features,
    ";qc_max_pct_mt=", max_pct_mt,
    ";normalize=LogNormalize;hvg=vst(2000);nn=rann;cluster=leiden(res=0.5)"
  )
)

jsonlite::write_json(out, out_json, auto_unbox = TRUE, pretty = TRUE)
cat(paste0("OK: wrote ", out_json, "\n"))
