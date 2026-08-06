#!/usr/bin/env Rscript

args <- commandArgs(trailingOnly = TRUE)

get_arg <- function(name, default = NULL) {
  prefix <- paste0("--", name, "=")
  for (a in args) {
    if (startsWith(a, prefix)) return(sub(prefix, "", a))
  }
  return(default)
}

in_rds <- get_arg("rds", "data/references/allen_cortex/allen_cortex.rds")
out_root <- get_arg("out-root", "data/references/allen_cortex/prepared")
assay_name <- get_arg("assay", "RNA")
label_col <- get_arg("label-col", "subclass")
gzip_outputs <- as.integer(get_arg("gzip", "1"))

if (!file.exists(in_rds)) stop(paste("Input RDS not found:", in_rds))

if (!requireNamespace("Matrix", quietly = TRUE)) stop("Missing R package 'Matrix'.")
if (!requireNamespace("Seurat", quietly = TRUE)) stop("Missing R package 'Seurat'. Install it in the container image before running.")
if (!requireNamespace("jsonlite", quietly = TRUE)) stop("Missing R package 'jsonlite'.")

options(bspm.sudo = TRUE)

dir.create(out_root, recursive = TRUE, showWarnings = FALSE)
mat_dir <- file.path(out_root, "filtered_feature_bc_matrix")
dir.create(mat_dir, recursive = TRUE, showWarnings = FALSE)

obj <- readRDS(in_rds)

if (!("Seurat" %in% class(obj))) {
  stop(paste("Expected a Seurat object; got classes:", paste(class(obj), collapse = ",")))
}

if (!(label_col %in% colnames(obj@meta.data))) {
  stop(paste("label-col not found in meta.data:", label_col))
}

if (!(assay_name %in% names(obj@assays))) {
  stop(paste("assay not found in object assays:", assay_name))
}

get_counts <- function(seurat_obj, assay) {
  # Seurat v5 uses `layer="counts"`; older Seurat uses `slot="counts"`.
  out <- tryCatch(Seurat::GetAssayData(seurat_obj, assay = assay, layer = "counts"), error = function(e) NULL)
  if (!is.null(out)) return(out)
  out <- tryCatch(Seurat::GetAssayData(seurat_obj, assay = assay, slot = "counts"), error = function(e) NULL)
  if (!is.null(out)) return(out)
  stop("Failed to retrieve counts from Seurat object (neither layer nor slot interface worked).")
}

counts <- get_counts(obj, assay_name)
if (!inherits(counts, "dgCMatrix")) counts <- as(counts, "dgCMatrix")

barcodes <- colnames(counts)
genes_raw <- rownames(counts)
if (is.null(barcodes) || length(barcodes) == 0) stop("Counts matrix missing colnames (barcodes).")
if (is.null(genes_raw) || length(genes_raw) == 0) stop("Counts matrix missing rownames (genes).")

# Ensure feature names are non-empty and unique (10x expects stable rows).
genes_raw[genes_raw == ""] <- "NA"
genes <- make.unique(genes_raw)
rownames(counts) <- genes

labels <- as.character(obj@meta.data[[label_col]])
names(labels) <- rownames(obj@meta.data)

# Align labels to matrix barcodes and drop unlabeled cells.
if (!all(barcodes %in% names(labels))) {
  missing <- sum(!(barcodes %in% names(labels)))
  stop(paste0("Label table missing ", missing, " matrix barcodes."))
}

lab_aligned <- labels[barcodes]
keep <- !(is.na(lab_aligned) | lab_aligned == "" | lab_aligned == "NA")
if (sum(keep) < 100) stop("Too few labeled cells after filtering (<100).")
counts <- counts[, keep, drop = FALSE]
barcodes <- colnames(counts)
lab_aligned <- lab_aligned[keep]

# RCTD (spacexr) rejects cell type names containing '/', so normalize.
# Keep the label meaning but ensure runner compatibility.
lab_aligned <- trimws(lab_aligned)
lab_aligned <- gsub("/", "_", lab_aligned, fixed = TRUE)

# Write Matrix Market + features + barcodes (gz optional)
mtx_path <- file.path(mat_dir, "matrix.mtx")
feat_path <- file.path(mat_dir, "features.tsv")
bc_path <- file.path(mat_dir, "barcodes.tsv")

Matrix::writeMM(counts, file = mtx_path)
utils::write.table(
  data.frame(gene_id = genes, gene_symbol = genes, stringsAsFactors = FALSE),
  file = feat_path,
  sep = "\t",
  quote = FALSE,
  row.names = FALSE,
  col.names = FALSE
)
writeLines(barcodes, con = bc_path, sep = "\n")

if (!is.na(gzip_outputs) && gzip_outputs == 1) {
  gz <- function(p) {
    con_in <- file(p, "rb")
    on.exit(close(con_in), add = TRUE)
    con_out <- gzfile(paste0(p, ".gz"), "wb")
    on.exit(close(con_out), add = TRUE)
    repeat {
      buf <- readBin(con_in, what = "raw", n = 1024 * 1024)
      if (length(buf) == 0) break
      writeBin(buf, con_out)
    }
    file.remove(p)
  }
  gz(mtx_path)
  gz(feat_path)
  gz(bc_path)
}

# Write labels TSV (barcode -> label) at out_root.
labels_tsv <- file.path(out_root, "reference_labels.tsv")
lab_df <- data.frame(barcode = barcodes, label = lab_aligned, stringsAsFactors = FALSE)
utils::write.table(lab_df, file = labels_tsv, sep = "\t", quote = FALSE, row.names = FALSE, col.names = TRUE)

# Minimal metadata for auditability (hash the input file via sha256sum when available).
sha <- ""
if (nzchar(Sys.which("sha256sum"))) {
  sha <- tryCatch(system(paste("sha256sum", shQuote(in_rds), "| awk '{print $1}'"), intern = TRUE)[1], error = function(e) "")
} else if (nzchar(Sys.which("shasum"))) {
  sha <- tryCatch(system(paste("shasum -a 256", shQuote(in_rds), "| awk '{print $1}'"), intern = TRUE)[1], error = function(e) "")
}

meta <- list(
  input_rds = in_rds,
  input_rds_sha256 = sha,
  assay = assay_name,
  label_col = label_col,
  label_normalization = "trimws; replace '/' with '_' (RCTD compatibility)",
  n_genes = as.integer(nrow(counts)),
  n_cells = as.integer(ncol(counts)),
  n_labels = as.integer(length(unique(lab_aligned))),
  out_root = out_root,
  matrix_dir = mat_dir,
  labels_tsv = labels_tsv
)

jsonlite::write_json(meta, file.path(out_root, "prepared_meta.json"), auto_unbox = TRUE, pretty = TRUE)

cat(paste0("OK: prepared 10x reference under ", out_root, "\n"))
cat(paste0("OK: matrix_dir=", mat_dir, "\n"))
cat(paste0("OK: labels_tsv=", labels_tsv, "\n"))
