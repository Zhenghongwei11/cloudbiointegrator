# CloudBioIntegrator: A standardized framework exposing method-dependent discordance and perturbation sensitivity in scRNA-seq and Visium analyses

**Short title:** Verifiable cross-method comparison for single-cell workflows

**Authors:** Hongwei Zheng^1

**Affiliations:**
1. School of Medicine, Quanzhou Medical College, Quanzhou, Fujian, People’s Republic of China

**Corresponding author:** Hongwei Zheng (2010015@qzmc.edu.cn)

## Abstract

**Background:** Method choice and routine analytical decisions can change biological conclusions in single-cell RNA-seq (scRNA-seq) and spatial transcriptomics. Benchmarking studies rank methods on aggregate metrics, but the discordance itself is rarely exposed where biologists look, at the level of individual spots or clusters, and sensitivity to routine preprocessing choices is seldom reported as evidence.

**Methods:** We developed CloudBioIntegrator, a containerized framework that runs standardized scRNA-seq and 10x Visium modules (Scanpy, Seurat v5, Harmony, scVI, RCTD, Tangram, and optional cell2location) under unified preprocessing, explicit parameters, and fixed intermediate tables. On public 10x datasets we quantified cross-method agreement at spot resolution, assessed sensitivity to predeclared perturbations (40 of 51 perturbation rows carried explicit stability flags), recorded runtime and failure behavior across 121 runs, and anchored reported artifacts to verifiable digests.

**Results:** On a mouse-brain Visium dataset (n=2,695 spots), RCTD, Tangram, and cell2location produced method-dependent spot-level composition vectors (pairwise cosine medians 0.364–0.633). Discordance concentrated in transcriptionally mixed and glia-associated spots, and the most discordant spot (cosine 0.006) was assigned to Meis2 by RCTD (weight 0.63) but to Serpinf1 by Tangram (weight 0.57) despite no Serpinf1 transcripts in the target or its six nearest neighbors. A permutation null model showed RCTD–Tangram agreement above chance (p<0.001) and chance-level agreement for both cell2location pairs (p=0.18–0.20). Of 40 threshold-flagged perturbation evaluations, 17 fell below stability thresholds (11 integration-vs-baseline rows are reported as deltas without pass flags), most under reduced feature selection. Under standardized inputs, Scanpy and Seurat baselines were nearly identical (2699 vs 2698 cells after QC; 5 vs 5 clusters), and Harmony improved batch mixing (0.127 vs 0.002) while preserving cluster structure (ARI mean 0.808; bootstrap 95% CI: 0.804–0.817). Operationally, 102/121 runs succeeded (success rate 0.843; Wilson 95% CI: 0.768–0.897), with the longer deconvolution runs exposing most failures. For the declared minimal pipeline, output artifacts were byte-identical (SHA-256) across three independent environments, including a public cloud Linux container; 90 run-level records were archived with per-artifact digests.

**Conclusions:** Method-dependent discordance and perturbation sensitivity are not noise; they can be localized, quantified, and verified. CloudBioIntegrator provides a narrow, reviewer-facing layer for doing so under unified preprocessing, alongside a literature-based scope statement against adjacent systems.

**Keywords:** single-cell RNA-seq; spatial transcriptomics; Visium; deconvolution; robustness; reproducibility; provenance

## Author Summary

Single-cell and spatial analyses depend on choices: which integration method, which deconvolution method, how many features, what random seed. These choices are usually invisible in a published conclusion. We built CloudBioIntegrator, a framework that runs standard single-cell and spatial modules under one fixed preprocessing regime and reports, in the same package, how much methods disagree at spot resolution, whether conclusions survive routine perturbations, how long runs take, and how often they fail. On a public mouse-brain Visium dataset, three mainstream deconvolution methods gave method-dependent composition estimates, with the largest disagreements concentrated in transcriptionally mixed and glia-associated spots; one example spot was inspectable against raw UMI counts. Across 40 threshold-flagged perturbation evaluations, 17 fell below stability thresholds. The same framework verified that its declared minimal pipeline produces byte-identical artifacts across three independent environments, so the discordance and robustness findings are not explained by environment noise. The goal is not to claim a best method, but to make method-dependent uncertainty explicit and independently checkable.

## Main Results Snapshot (Table 1)

| Evaluation axis | Quantitative summary | Supporting evidence |
| --- | --- | --- |
| Visium cross-method discordance | RCTD vs Tangram spotwise cosine median 0.364 (IQR 0.234–0.495); pairwise medians 0.364–0.633 across method pairs; concordance lower in glia-associated spots (median 0.288 vs 0.377), n=2,695 spots; RCTD–Tangram agreement above chance (permutation p<0.001) while cell2location pairs were at chance level (p=0.18–0.20) | Figure 4; Supplementary Tables S1, S5 and S6 |
| Robustness under perturbation | 17 of 40 threshold-flagged evaluations failed predefined stability thresholds (11 integration-vs-baseline rows are deltas without pass flags), most under reduced feature selection (10 failures with ARI<0.90 under HVG/2) | Figure 6; Supplementary Table S3 |
| Operational reliability | 102/121 runs succeeded\*; run success rate 0.843 (Wilson 95% CI: 0.768–0.897); Visium deconvolution modules dominate recorded failures | Figure 5; Supplementary Tables S1 and S3 |
| Minimal external comparison | Nominal Scanpy and Seurat PBMC 3k outputs were nearly identical under the same declared inputs (2699 vs 2698 cells after QC; 5 vs 5 clusters) | Table 2 |
| scRNA integration comparability | Harmony batch-mixing fraction 0.002→0.127; ARI vs baseline mean 0.808 (bootstrap 95% CI: 0.804–0.817) | Figure 3; Supplementary Table S1 |
| Verification of reported artifacts | Run-scoped minimal-path artifacts byte-identical (SHA-256) across three independent environments, including a public cloud Linux container; 90 run-level records archived with per-artifact digests | Figure 2; Supplementary Table S1 |

\*Operational totals include 1 internal toy ingest sanity-check run used only for input-interface validation.

## Introduction

Single-cell RNA-seq (scRNA-seq) and spatial transcriptomics have become foundational for studying cell states and tissue organization, but current analytical pipelines remain fragile. Results can shift with seemingly minor changes in compute environment or default settings. Most critically, opaque defaults in heuristic steps (e.g., highly variable gene selection and neighbor-graph construction) can alter batch-correction metrics or inferred cell-type proportions in downstream spatial mapping. These sources of drift slow iteration, complicate collaboration, and create friction in peer review, where reviewers increasingly ask for reruns, sensitivity analyses, and clearer provenance.

In practice, “reproducibility” often reduces to sharing scripts and an environment specification. For modern scRNA-seq and spatial pipelines, this is often insufficient: the same conceptual workflow can yield different clustering, annotation, or deconvolution outputs depending on hidden defaults, environment details, and untracked parameter choices. These shifts are not merely cosmetic; they can alter downstream biological narratives (e.g., inferred cell-type proportions, batch structure, or spatial composition maps), while operational realities (time-to-result, failure rates, and common failure modes) are rarely reported.

To quantify this directly, we systematically profile environment-induced technical variability under predeclared perturbations. In our robustness trials, 17 of 40 threshold-flagged evaluations failed to meet stability thresholds under mild perturbations (e.g., reduced feature selection), illustrating how sensitive common conclusions can be to routine analytical variation.

We address this by treating auditable standardization, rather than mere packaging, as the primary design objective. Instead of ad hoc scripts, CloudBioIntegrator provides a curated workflow framework in which each analytical module has a declared preprocessing regime, explicit parameterization, and standardized outputs. Reproducibility and verification are supported by a compact reproducibility package (Supplementary Data 1) that records an environment identifier, declared parameters, and SHA-256 digests for manuscript-linked artifacts.

We evaluate CloudBioIntegrator on canonical public 10x datasets spanning scRNA-seq and Visium [1-6]. Our workflow modules include widely used baselines (Scanpy [7], Seurat v5 [8]) and commonly adopted advanced components when appropriate (Harmony [9], scVI [10], RCTD [11], Tangram [12], and optional cell2location [13]). To make the manuscript’s value proposition explicit, we also position the framework against adjacent systems that address only part of the same problem space, including workflow platforms, toolkit-based analyst workflows, and benchmark resources. Metrics and leakage protections are predeclared, and results are recorded as tables rather than only as plots, reducing the opportunity for cherry-picking.

This paper focuses on research workflows and is limited to scRNA-seq and 10x Visium analyses on public benchmark datasets. We do not claim clinical utility, causal inference, or therapeutic impact. Instead, we demonstrate three contributions: (i) cross-method discordance at spot resolution that is locally inspectable against raw transcript evidence, (ii) perturbation sensitivity as first-class evidence, showing that routine analytical choices flip a substantial fraction of conclusions below stability thresholds, and (iii) artifact verification showing that this discordance is not environment noise, with output artifacts byte-identical on the declared minimal pipeline across three independent environments.

## Contributions

1. Under unified preprocessing, mainstream Visium deconvolution methods produce method-dependent spot-level composition estimates (pairwise cosine medians 0.364–0.633 on a mouse-brain dataset, n=2,695 spots); discordance concentrates in transcriptionally mixed and glia-associated spots and is locally inspectable against raw UMI counts at individual spots.
2. Mild predeclared perturbations (reduced feature selection, seed changes) flip 17 of 40 threshold-flagged evaluations below predefined stability thresholds, and operational feasibility (runtime, failure modes, compute gating) is reported alongside biological summaries as a decision-relevant dimension.
3. Artifact verification links every reported artifact to a SHA-256 digest: output artifacts are byte-identical on the declared minimal pipeline across three independent environments, and an archive of 90 run records documents what ran, so the discordance and robustness findings can be checked without rerunning the analysis.

## Related Work

Reproducible computational biology has long been supported by workflow engines, community-curated pipelines, and environment managers (e.g., Snakemake [14], Bioconda [15], BioContainers [16], and Dockstore [17]). Containers and execution isolation are widely used to stabilize environments across compute settings (e.g., Singularity [18]). Platform ecosystems such as Galaxy emphasize accessible, reproducible, and collaborative analyses [19-21], workflow execution services aim to encourage reuse across workflow languages [22], and community-curated ecosystems such as nf-core standardize pipeline implementations at scale [23]; cloud-based single-cell execution frameworks such as Cumulus address scalability and cost [24]. However, these layers primarily orchestrate execution; they do not, by themselves, standardize algorithm-internal defaults or guard against nondeterminism within methods. For example, a workflow can be rerun faithfully while still allowing shifts in method-specific defaults (e.g., HVG selection rules or neighbor-graph parameters) that change downstream clustering and interpretation. Beyond tooling, reproducibility frameworks emphasize shareable data/metadata and clear reporting (e.g., FAIR principles [25] and reproducible computing rules/best practices [26-30]).

Workflow provenance and packaging standards are also increasingly emphasized as part of reproducible review: examples include interoperable provenance patterns such as CWLProv [31] and artifact packaging approaches such as RO-Crate [32].

In the single-cell ecosystem, widely adopted analysis toolkits such as Scanpy [7] and Seurat v5 [8] offer comprehensive workflows but still leave room for analyst-specific variation and hidden defaults. The Bioconductor ecosystem similarly emphasizes orchestrated, modular single-cell analyses with an explicit focus on reproducibility [33]. Benchmarking resources emphasize explicit evaluation designs, metric taxonomies, and transparent reporting of trade-offs (e.g., scIB [34]). For spatial transcriptomics, frameworks such as Squidpy provide representative software-paper conventions and analysis primitives [35].

Reproducibility is also impacted by numerical stability and perturbations across environments and dependency stacks; recent work proposes practical approaches to localize and characterize such perturbations in pipelines [36]. For single-cell studies, reproducible benchmarking and scalable evaluation patterns have been proposed to reduce ad hoc reporting (e.g., MetaNeighbor-centered reproducible analysis patterns) [37].

For spatial transcriptomics, tooling spans general-purpose analysis frameworks and specialized mapping/deconvolution methods. Representative methods used in this work include RCTD [11] and Tangram [12], and we optionally support cell2location [13] for probabilistic mapping when compute permits. These methods highlight a general issue: as the set of available methods evolves quickly, reproducibility and reviewability hinge not only on code availability but also on standardized preprocessing, explicit provenance, and verifiable artifacts.

Recent spatial tooling and evaluation literature also emphasizes uncertainty, resolution, and benchmarking discipline (e.g., BayesSpace [38], SPOTlight [39], CARD [40], Giotto [41], and comparative deconvolution benchmarks [42,43]).

CloudBioIntegrator is positioned at the intersection of these themes. It contributes an auditable workflow framework and a reproducibility packaging standard for scRNA-seq and Visium analyses, focusing on method-specific parameter stability under standardized preprocessing. Relative to adjacent systems, our design goal is not to replace workflow engines or benchmarking resources, but to combine their most review-relevant elements for a narrower dual-modality use case: standardized intermediate tables, runtime/failure visibility, predeclared robustness audits, and manuscript-linked artifact verification. In other words, the framework is intended to make “what ran and what it produced” legible to reviewers and users: a single package contains an environment identifier, exact parameterization, source tables, and the main figures, with SHA-256 digests that can be verified independently.

Additional context and a structured comparison to adjacent systems is provided in the Supplementary Materials.

## Materials and Methods

### System Overview and Standardized Method Library

CloudBioIntegrator is a framework for packaging scRNA-seq and Visium analyses as standardized, versioned workflow modules (analytical modules) executed in pinned containers. Each run produces the same set of structured outputs (tables and figures) under an explicit preprocessing regime, making comparisons between modules and across environments interpretable. A compact reproducibility package is produced for every run, recording an environment identifier, declared parameters, and SHA-256 digests for manuscript-linked artifacts.

Implementation specifications (pipeline design, research design, and figure standards) are included in the accompanying Supplementary Materials. Supplementary implementation details (reproducibility package layout, perturbation definitions, and effect-size computations) are provided in the Appendix.

### Standardized Bioinformatic Assays and Parameter Constraints

CloudBioIntegrator is intentionally not a general-purpose analysis notebook. It executes a fixed set of standardized analytical modules for ingest, QC, clustering, annotation, integration, spatial mapping, figure export, and packaging. This “frozen protocol” enforces a standardized preprocessing regime and minimizes variability that can arise from manual parameter tuning, while ensuring that the primary artifacts needed for review are produced consistently across runs.

Failure behavior is part of the protocol. When required inputs are missing (e.g., an appropriate reference scRNA dataset for spatial mapping), the workflow raises explicit errors and records the failure mode. When advanced methods require additional compute (GPU tier), those steps are gated explicitly rather than silently changing behavior. This is especially important for GPU-accelerated methods where strict bitwise determinism is not expected; in such cases, we emphasize traceability (environment identifiers, declared parameters, and SHA-256 digests of produced artifacts).

A compact reproducibility package is produced for every run; its layout, checksum scheme, and digest-based verification workflow are specified in Appendix A2 and released as Supplementary Data 1.

### Data Sources and Preprocessing Pipeline

The pipeline starts from standard 10x Genomics outputs.

- scRNA-seq: count matrix outputs from the 10x Cell Ranger workflow (e.g., PBMC 3k, PBMC 10k, and integration anchors)
- Visium: count matrix and spatial coordinate outputs from the 10x Space Ranger workflow

Dataset metadata and acquisition details (sources and SHA-256 digests) are provided in the Supplementary Materials (Supplementary Data 1).

#### Dataset governance and licensing (required for submission)

We use public datasets selected to cover the two supported modalities (scRNA-seq and Visium) and to enable end-to-end reproduction from standard 10x inputs. Dataset-level metadata used for the paper (organism, tissue, and sample size proxies such as cells/spots) are summarized in the Supplementary Materials.

For this manuscript, we distinguish three roles: (i) smoke-test datasets (to validate that the pipeline can ingest a real 10x matrix end-to-end), (ii) primary benchmark datasets (to evaluate outputs and operational behavior for the main claims), and (iii) planned replication datasets (to broaden generalizability). The evaluation matrix is predeclared and included in the Supplementary Materials.

In addition, we include 1 internal toy ingest sanity-check run used only for input-interface validation; this run is included in the operational-run totals (Table 1).

### scRNA Method Library (v0)

The scRNA method library includes CPU-first baselines and resource-aware advanced modules. Baselines implement standard workflows in Scanpy [7] and Seurat v5 [8] (QC, normalization, HVG selection, dimensionality reduction, graph construction, Leiden clustering, and UMAP visualization). Optional annotation uses CellTypist [44]. Advanced modules include Harmony [9] integration comparisons against baseline embeddings, and optional scVI [10] integration when compute permits.

Full method inventories and parameter notes are provided in the Supplementary Materials. In brief, baseline scRNA processing records QC and clustering summaries, and optional annotation uses CellTypist [44] with within-run concordance checks (e.g., NMI). The baseline method choices rely on standard components (e.g., Leiden [45] and UMAP [46]); additional details on preprocessing defaults and integration trade-offs are provided in the Appendix.
As contextual related work, widely used scRNA preprocessing extensions (pooling-based normalization [47], empty-droplet detection [48], doublet detection [49], and SCTransform variance stabilization [50]) and alternative integration strategies (e.g., Scanorama [51]) are not the focus of the main claims, but are relevant to interpreting portability and robustness across labs.

### Visium Method Library (v0.1)

The Visium library includes baseline spatial QC and summary modules (including coordinate-graph clustering) and mapping/deconvolution modules. Deconvolution weights are generated using RCTD [11] and Tangram [12], with optional cell2location [13] when GPU-tier compute is available. For numerical stability, the cell2location module applies bounded sampling guards to rate parameters during variational inference; these guards and their parameter values are recorded in the run metadata. Outputs include spatial maps, uncertainty proxies, and cross-method concordance summaries (F4).

Full Visium method inventories and implementation notes are provided in the Supplementary Materials.

Implementation summary: we ingest Space Ranger outputs and compute spot-level QC and coordinate-graph clustering. For mapping/deconvolution, we generate spot×cell-type weight tables for RCTD [11] and Tangram [12], and optionally for cell2location [13] when GPU-tier compute is available. These outputs are used to render spatial maps, uncertainty proxies, and concordance summaries in F4. To support cross-language integration, the method library uses standardized intermediate representations (matrix- and metadata-style tables) as the module interface, rather than passing language-specific in-memory objects between runtimes.

### Evaluation Metrics and Leakage Protections

Metrics and leakage protections are predeclared and included in the Supplementary Materials. We report method benchmarks, concordance checks, runtime/failure metrics, and robustness trials in standardized tables provided in Supplementary Data 1.

#### Benchmark design: the evaluation matrix

We define an explicit evaluation matrix (tasks × datasets × methods × metrics). This matrix is the source of truth for what was evaluated and which evidence modules support each claim, and it is included in the Supplementary Materials.

We predeclare which methods and metrics are required vs optional for each task/dataset. This prevents post-hoc method selection and provides a clear “what ran” record for peer review. Metrics are recorded as table rows and released as Supplementary Data. Where uncertainty is computed (e.g., bootstrap intervals for concordance metrics, Wilson intervals for proportions), effect sizes and uncertainty summaries are reported in Supplementary Table S1.

Cross-method agreement for Visium is quantified as spotwise cosine similarity between normalized composition vectors, and its statistical calibration is assessed with a permutation null model in which one method's composition vectors are randomly reassigned to spots (1,000 permutations; fixed seed; Supplementary Table S1). The batch-mixing fraction for integration is, for each cell, the proportion of its k=15 nearest neighbors (k-NN connectivities graph, self excluded) that belong to a different batch; the reported value is the mean of these per-cell fractions, with exact neighborhood parameters recorded in the evaluation matrix. Robustness flags use pragmatic decision thresholds (ARI>=0.90 for seed and HVG perturbations; mean Pearson>=0.90 for Tangram HVG/2 concordance); these thresholds are intended to surface instability rather than to serve as statistical significance bounds.

Leakage protections are operationalized by restricting which artifacts are used for evaluation. For example, comparisons between annotation labels and clusters are explicitly framed as within-run concordance checks rather than ground-truth validation.

Compute tiers are treated as part of the benchmark design. CPU-first baselines are always runnable in portable environments, while GPU-tier methods are gated and explicitly labeled as such; per-run runtime and failure outcomes are recorded to surface feasibility constraints. This is critical for fair comparisons: advanced methods may improve certain metrics but may not be practical under constrained compute.

### Adjacent-System Positioning and Minimal External Comparison

To make the scope of our claims explicit, we compared CloudBioIntegrator to four adjacent system types drawn from the literature cited in the manuscript: a biomedical analysis platform (Galaxy [19-21]), a workflow engine (Snakemake [14]), analyst-built toolkit workflows centered on Scanpy/Seurat [7,8], and a benchmark resource (scIB [34]). The positioning matrix uses only properties directly relevant to the manuscript’s claims: support for scRNA-seq and Visium, whether preprocessing and output tables are standardized natively or only through user customization, and whether the system natively exposes cross-method benchmarking, runtime/failure logs, robustness audits, artifact-level checksums, and reviewer-ready bundles.

The empirical comparison is intentionally limited to method layers that already exist in our benchmark outputs rather than to whole external platforms. Specifically, we summarize (i) nominal Scanpy versus Seurat baseline outputs on PBMC 3k, (ii) standardized integration trade-offs across baseline embeddings, Harmony, and scVI on the public PBMC integration anchor, and (iii) standardized Visium trade-offs across RCTD, Tangram, and cell2location on the mouse-brain benchmark. These summaries are derived directly from the benchmark and runtime tables released in Supplementary Data 1 and are included to contextualize our claims rather than to claim universal superiority.

### Fidelity Verification and Reproducibility Protocols

Each run produces a reproducibility package and a reproducibility table capturing environment identifiers and key output hashes. Figure provenance is captured as part of the reproducibility package (Supplementary Data 1).

Verification is digest-based: manuscript-linked tables and figures are distributed with SHA-256
digests, so readers can validate artifacts without rerunning the analysis (Appendix A2). A
pinned-commit fresh-VM reproduction checklist and the SHA-256-verified review bundle are provided in
the Supplementary Materials (Appendix A6; Supplementary Data 1).

## Supplementary Methods (Appendix)

### A1. Defined Operation Set, Parameterization, and Determinism Controls

CloudBioIntegrator executes a predefined set of analytical operations (ingest, QC, clustering, annotation, integration, spatial mapping, figure export, reproducibility packaging). The defined operation set and module implementations are versioned as part of the pipeline specification and are documented in the Supplementary Materials. Analytical modules are treated as the unit of reproducible behavior.

To make runs comparable across reruns, we record:
- a parameter hash derived from declared inputs and parameters,
- an environment identifier derived from the execution environment,
- and per-output-table SHA-256 digests.

Where strict bitwise determinism is not practical (notably for GPU-tier methods), the protocol prioritizes traceability (environment identifiers and SHA-256 digests for produced artifacts) and explicitly records compute tier and runtime metadata.

### A2. Reproducibility Bundles and Verification

Each run produces a reproducibility package that is designed to be reviewer-facing rather than developer-facing. At minimum it contains environment metadata and an environment identifier, a SHA-256 manifest for key outputs, the primary results tables used by the manuscript, and the exported main figures.

The central verification workflow is digest-based: readers can validate that manuscript-linked tables and figures match the SHA-256-verified artifacts, without rerunning analysis code. Figure-to-table traceability is provided in the reproducibility package (Supplementary Data 1).

### A3. Benchmark Contract and Evidence Gating

We predeclare what will be evaluated using an evaluation matrix (Supplementary Table S2). Each row specifies the task, dataset, workflow module configuration, and required metrics. This provides two safeguards:
- it prevents post‑hoc method selection by making “what ran” explicit,
- and it limits manuscript claims to rows with implemented evidence.

Leakage protections are applied at the level of which artifacts are compared and how they are interpreted. In particular, cluster–label concordance metrics are treated as within-run plausibility/stability checks rather than as ground-truth accuracy measures.

### A4. Robustness Perturbations and Threshold-Based Flags

Robustness is evaluated using predeclared perturbations and recorded as a matrix (Supplementary Data 1). Perturbations include:
- `seed_plus_1` (random-seed sensitivity of clustering),
- `hvg_half` (feature-selection sensitivity; HVG vs HVG/2),
- and integration-vs-baseline comparisons for batch mixing.

Threshold-based flags are conservative and are intended to surface brittleness rather than to “grade” methods. For example, several failures are triggered by ARI<0.90 under HVG/2 perturbation, indicating that a method’s clustering is not stable under reduced feature selection.

### A5. Effect Sizes and Uncertainty

We report effect sizes with uncertainty for the main claims in Supplementary Table S1. Proportions (e.g., reproducibility pass rate; run success rate) use Wilson 95% confidence intervals; mean concordance metrics use nonparametric bootstrap intervals. The computation code is included in the reproducibility package for transparency and reproducibility.

### A6. Fresh-VM End-to-End Reproduction

The end-to-end reproduction protocol is provided in the Supplementary Materials. In brief, it pins a commit, fetches datasets from registered sources, runs the declared stages, regenerates figures, and builds a final reproducibility package. For submission, we provide a reviewer-facing reproducibility package with SHA-256 digests for manuscript-linked artifacts (Supplementary Data 1).

## Results

Table 1 provides a compact summary of the headline quantitative outcomes; each subsection below reports the corresponding evidence.

### Cross-Method Discordance in Visium Deconvolution Under Unified Preprocessing

We first asked how much mainstream deconvolution methods disagree when the input, preprocessing, and reference are held constant. Figure 4 compares RCTD [11], Tangram [12], and cell2location [13] under unified preprocessing on a mouse-brain Visium dataset (n=2,695 spots) using a tissue-matched Allen cortex scRNA reference with provided labels [52]. Because paired ground truth is not available, we quantify cross-method agreement directly: per-cell-type Pearson correlation between RCTD and Tangram is weak (mean 0.062; bootstrap 95% CI: 0.058–0.069), and spotwise cosine similarity between normalized composition vectors indicates limited agreement (RCTD vs Tangram median 0.364 [IQR 0.234–0.495]; Tangram vs cell2location median 0.633 [IQR 0.557–0.689]; RCTD vs cell2location median 0.494 [IQR 0.421–0.558]). This degree of divergence is directionally consistent with recent deconvolution benchmarks, which reported that spatial composition estimates can vary materially across model classes even when the same reference and tissue context are used [42,43]. We do not claim to be the first to observe cross-method disagreement; the increment here is that the disagreement is localized and inspectable.

To calibrate what “limited agreement” means, we compared the observed spotwise cosine distributions to a permutation null in which one method's composition vectors were randomly reassigned to spots (1,000 permutations; seed 20260806; Supplementary Table S1). RCTD versus Tangram agreement was significantly above chance (observed median 0.364 vs null median 0.323; permutation p<0.001), yet only 1.3% of spots exceeded the 99th percentile of the null, indicating that the above-chance signal is real but concentrated in a small fraction of spots. Tangram versus cell2location (observed 0.633 vs null 0.633; p=0.196) and RCTD versus cell2location (0.494 vs null 0.494; p=0.182) showed no spot-level correspondence beyond chance: their raw cosine values reflect shared marginal composition rather than spot-specific agreement.

The discordance is not uniform across the tissue. In a coarse stratification based on the top-weight cell-type label in the RCTD weight table, agreement between RCTD and Tangram was higher in spots whose top RCTD label corresponded to layer-like subclasses (labels starting with L1–L6; median 0.394 [IQR 0.278–0.515], n=1,726) than in non-layer-like spots (median 0.294 [IQR 0.156–0.438], n=969). Concordance was also lower in spots dominated by glia-associated labels (Astro, Oligo, OPC, Microglia/Macrophage, Endo/Peri/VLMC; median 0.288 [IQR 0.152–0.418], n=424) than in other spots (median 0.377 [IQR 0.251–0.503], n=2,271). Discordance therefore concentrates in transcriptionally mixed or weaker-signal contexts, where small preprocessing or modeling differences are amplified into different composition vectors.

At spot resolution the disagreement can be inspected against raw data. The most discordant spot in our table-level comparison (barcode ACGTGACAAAGTAAGT-1; cosine 0.006) is assigned predominantly to Meis2 (a marker gene used in mouse cortical taxonomies [53]) by RCTD (weight 0.63) but to Serpinf1 by Tangram (weight 0.57; Supplementary Table S5). Serpinf1 is commonly used as a marker for vascular leptomeningeal (VLMC) / perivascular fibroblast-like populations in molecular atlases of the brain vasculature and perivascular space [54]. Raw UMI counts at the same spot and its nearest-neighbor spots show detectable Meis2 transcripts (22 UMI at the target spot) while Serpinf1 is 0 across the target and its six nearest neighbors (Supplementary Table S6), supporting that this disagreement is not solely a minor modeling artifact. In this setting, Tangram's high weight on Serpinf1 despite a local absence of Serpinf1 transcripts suggests an over-smoothing or embedding-alignment artifact rather than a marker-supported assignment. Mechanistically, the divergence follows from the two models' objectives: RCTD fits a per-spot reference-based likelihood that responds to local marker abundance, whereas Tangram solves a global optimal-transport alignment in which spot compositions are coupled through an embedding smoothness prior, so a high weight can be assigned to a cell type even where its transcripts are locally absent. Without histological ground truth we do not adjudicate which method is “correct” at such spots; the point is that the disagreement is concrete, locally inspectable, and falsifiable against raw counts, which is exactly the information a downstream user needs before trusting a composition map built on a single method. By holding preprocessing, gene filtering, and intermediate tables constant, CloudBioIntegrator makes these differences explicit and comparable rather than leaving them as undocumented analyst choices. Figure 4 visualizes inferred cell-type patterns and reports uncertainty proxies (entropy of normalized weights; maximum weight) as compact summaries of assignment uncertainty. The finding is descriptive: on this dataset, method choice changes inferred composition in a material, localized, and inspectable way.

### Perturbation Sensitivity of Clustering and Deconvolution

We next asked whether routine analytical choices change conclusions that are otherwise reported as stable. Robustness trials make “stability under small changes” explicit. We predeclare perturbations (seed changes, feature-selection changes, and integration-vs-baseline comparisons) and record per-perturbation deltas and threshold-based flags. Across the recorded robustness matrix, 17 of 40 threshold-flagged evaluations failed to meet predefined robustness thresholds (11 integration-vs-baseline rows are deltas without pass flags) (Figure 6; Supplementary Table S3). The most common failure was instability under reduced feature selection: 10 failures with ARI<0.90 under the HVG/2 perturbation. Five failures reflected reduced Visium concordance under HVG/2 (mean Pearson<0.90 for Tangram HVG/2 sensitivity), and 2 failures reflected seed sensitivity (ARI<0.90 under seed+1 perturbation; full matrix in Supplementary Table S3).

The practical reading is direct: in 17 of 40 threshold-flagged evaluations (42.5%), halving the feature set or changing the random seed moved clustering or deconvolution outputs below a stability flag. Any single-run conclusion reported without such sensitivity context carries an unquantified risk of being an artifact of one analytical choice. These failures are treated as evidence of brittleness under realistic perturbations rather than as noise to be excluded.

### Operational Feasibility: Runtime, Failure, and Compute Gating

We then characterize operational behavior: how long runs take, how often they fail, and where failures concentrate. Across the 121 executed runs that reached a terminal outcome (success or failure), 102 completed successfully (run success rate 0.843; Wilson 95% CI: 0.768–0.897; summary in Supplementary Table S1; full run records in Supplementary Table S3). Of 139 recorded runs in total, 18 required GPU acceleration and were automatically bypassed when no GPU resource was available; the success rate is computed over the 121 runs that reached a terminal outcome. Failure modes are dominated by runtime errors and dependency issues, and the runtime–reliability trade-off varies substantially by workflow module (Figure 5). Ingestion and baseline scRNA steps complete quickly, whereas Visium deconvolution runs are up to 88-fold longer (median 1853.2 s for RCTD versus 21.0 s for Harmony) and are correspondingly more exposed to failure and environment fragility: median successful runtime was 1853.2 s for RCTD, 290.9 s for Tangram, and 481.9 s for cell2location, with success rates of 5/6, 5/5, and 4/16 respectively (Table 2).

The cell2location failures deserve a precise interpretation. Of its 12 failed attempts, 11 were recorded runtime errors and 1 was an early missing-dependency event (Supplementary Table S3), and successful runs tended to occur later in the run log, consistent with a narrower dependency and runtime tolerance window than the CPU-first baselines. GPU-tier methods are gated explicitly rather than silently downgraded, so compute-tier availability is part of the recorded evidence. Longer-running deconvolution modules have a larger “failure surface area” (memory pressure, toolchain/library compatibility, and, where applicable, GPU availability), which makes environment stabilization and explicit reporting of failure modes part of scientific usability rather than an implementation detail. We report these feasibility limits alongside the composition outputs so that analysts can separate biological disagreement from execution fragility when selecting a method for downstream interpretation.

### Standardized Modules Recover Baseline Behavior and Expose Integration Trade-Offs

The framework is a wrapper around established method layers, so we verified that it does not distort them. Under standardized inputs, nominal Scanpy [7] and Seurat v5 [8] baselines produced nearly identical PBMC 3k summaries: 2699 vs 2698 cells after QC, median total counts 2197.0 vs 2196.5, median genes 817 vs 816, and median cluster count 5 vs 5 (Table 2). We treat annotation- and integration-related concordance as stability/plausibility checks rather than as accuracy claims. For example, on PBMC benchmark datasets, the within-run concordance between CellTypist labels [44] and unsupervised clusters is high for PBMC 10k (NMI mean 0.838; bootstrap 95% CI: 0.832–0.841) and moderate for PBMC 3k (NMI mean 0.645; bootstrap 95% CI: 0.641–0.650; Supplementary Table S1). To make this concrete, in the PBMC 3k dataset a single Leiden cluster (cluster 1; n=678 cells) is enriched for canonical monocyte markers (e.g., CST3, LST1, LYZ, TYROBP) and is consistently annotated as monocytes by CellTypist: 636/678 cells (93.8%) receive a monocyte label (dominant label: Classical monocytes; Supplementary Table S4).

For integration, Harmony [9] substantially increases batch mixing compared to the baseline embedding (nearest-neighbor batch-mixing fraction 0.002→0.127 in the integration anchor), while preserving a similar clustering structure in the primary integration anchor (ARI between baseline and Harmony clusters: mean 0.808; bootstrap 95% CI: 0.804–0.817; Supplementary Table S1). scVI [10] also improved mixing (0.091) and remained close to Harmony (ARI mean 0.771; bootstrap 95% CI: 0.764–0.775). Median successful runtime was 21.0 s for Harmony and 42.8 s for scVI, with success rates of 11/11 and 20/24. This is consistent with an integration outcome that changes the embedding in a controlled way (improving mixing) without producing unrelated cluster assignments, and it complements anchor-based integration paradigms commonly used in Seurat workflows [55]. Table 2 summarizes the minimal external comparison used to anchor these claims.

| Table 2. Minimal empirical comparison against established method layers | Dataset | Methods | Key observations |
| --- | --- | --- | --- |
| Nominal scRNA baselines | PBMC 3k | Scanpy standard vs Seurat v5 standard | Median nominal outputs were nearly identical: 2699 vs 2698 cells after QC, median total counts 2197.0 vs 2196.5, median genes 817 vs 816, and median cluster count 5 vs 5. |
| Integration trade-offs | PBMC integration anchor | Baseline scanpy-standard vs Harmony vs scVI | Batch-mixing fractions (mean across cells of the k=15 nearest-neighbor mixing fraction) were 0.002, 0.127, and 0.091, respectively. Harmony stayed close to the baseline clustering structure (ARI 0.808, 95% CI 0.804-0.817), while scVI stayed close to Harmony (ARI 0.771, 95% CI 0.764-0.775). Median successful runtime was 21.0 s for Harmony and 42.8 s for scVI, with success rates 11/11 and 20/24. |
| Visium deconvolution trade-offs | Mouse Brain Visium | RCTD vs Tangram vs cell2location | Pairwise spotwise cosine medians ranged from 0.364 to 0.633 across method pairs. Median successful runtime was 1853.2 s for RCTD, 290.9 s for Tangram, and 481.9 s for cell2location, with success rates 5/6, 5/5, and 4/16. |

### Verification of Reported Artifacts

The discordance and robustness findings above are only meaningful if they reflect method behavior rather than environment noise. To support that reading, we verified byte-level stability on the declared minimal pipeline (pipeline integrity validation and real 10x matrix ingestion): output artifacts produced identical SHA-256 digests across three independent environments, including the local containerized runtime used in this study, a second container stack with a different OS/Python combination, and a public cloud Linux container (manifest archived with the released reproducibility package). In parallel, all 90 run-level reproducibility records are archived with per-artifact digests for traceability (Figure 2). Two scope qualifications apply. Byte-identity was verified on the declared minimal pipeline rather than on every analysis module; for the full analysis graph, the reproducibility package provides traceability (digests recorded at write time, environment identifiers, declared parameters) rather than a determinism proof. The archive of 90 run records likewise documents what ran and the digest of each output at write time; it is a traceability record, not evidence of cross-run determinism by itself. Readers can validate released artifacts without rerunning the analysis (Supplementary Data 1).

### Scope and Positioning Against Adjacent Systems

To make the scope of our claims explicit, Table 3 summarizes an author-assessed feature inventory across adjacent system types. The matrix records documented capabilities rather than measured performance; it delimits the framework's niche without ranking systems. Workflow engines stabilize execution, toolkit workflows provide flexible analyses, and benchmark resources formalize selected comparisons; CloudBioIntegrator is not a replacement for any of them, but a layer that couples standardized dual-modality preprocessing to cross-method comparison, perturbation sensitivity, failure logging, and artifact-level provenance.

| Table 3. Author-assessed feature inventory of adjacent system types (documented capability, not measured performance) | Layer | scRNA | Visium | Standardized tables | Built-in benchmarking | Failure logging | Robustness audit | Artifact digests | Reviewer bundle |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Galaxy [19-21] | Analysis platform | Yes | Tool-dependent | Partial | No | Partial | No | No | No |
| Snakemake [14] | Workflow engine | User-defined | User-defined | User-defined | No | Partial | No | User-defined | User-defined |
| nf-core/Nextflow [23] | Community workflow ecosystem | Via pipelines | Via pipelines | Via pipelines | No | Partial | No | Via pipelines | Via pipelines |
| Scanpy/Seurat analyst-built workflows [7,8] | Toolkit workflow | Yes | Partial | User-defined | No | No | No | No | No |
| scIB [34] | Benchmark resource | Yes | No | Yes | Yes | No | Partial | No | No |
| CloudBioIntegrator | Standardized workflow framework | Yes | Yes | Yes | Yes | Yes | Yes | Yes | Yes |

## Discussion

The central finding of this study is that, under one standardized preprocessing regime, mainstream Visium deconvolution methods disagree at spot resolution in ways that are locally inspectable, and that a substantial fraction of routine analytical perturbations flips conclusions below stability thresholds. Existing deconvolution benchmarks have already established that methods rank differently under aggregate accuracy metrics [42,43]. What they do not typically provide is the inspection layer we emphasize here: where exactly two methods disagree, whether that disagreement is supported or contradicted by raw transcript evidence at the spot, and whether the conclusion survives perturbations that a typical analyst might apply without reporting. The permutation null model adds an important calibration: RCTD–Tangram agreement is above chance but weak (median 0.364 vs null 0.323; p<0.001), while both cell2location pairs show no spot-level correspondence beyond chance (p=0.18–0.20). Raw cosine values alone would have overstated agreement for the cell2location pairs; the null model is what exposes the distinction. The contribution is not the observation of cross-method disagreement itself, which the benchmark literature already establishes, but its combination with spot-level inspectability, perturbation sensitivity as first-class evidence, and independently verifiable artifacts.

### Method-Dependent Uncertainty Is Local and Inspectable

For Visium deconvolution, the absence of paired ground truth means the most defensible claim is disciplined reporting: spatial maps, uncertainty proxies, and concordance metrics that make method-dependent differences explicit. The discordance pattern itself carries information. Agreement was systematically higher in layer-like spots and lower in glia-dominated and transcriptionally mixed contexts, which is consistent with disagreement amplifying where signal-to-noise is weak or cell-state boundaries are dense. The Meis2/Serpinf1 example shows that a discordant assignment can be examined against raw UMI counts rather than left as an opaque model output. We do not adjudicate which method is correct at that spot; we show that the question can be asked at spot resolution, which aggregate benchmarks are not designed to expose.

### Robustness Deserves the Same Reporting Status as Accuracy

In this study, 17 of 40 threshold-flagged evaluations fell below stability thresholds, most under a 50% reduction in feature count (HVG/2). This reflects a property of the task–method combination that a point estimate cannot reveal, rather than a defect of any single method. Reporting robustness flags alongside the nominal result converts a hidden analytical risk into a documented one. When flags indicate instability under seed or HVG perturbations (e.g., ARI<0.90 under HVG/2), we recommend that users (i) rerun with multiple seeds to assess sensitivity, (ii) explicitly vary HVG definitions and verify that key conclusions are stable, and (iii) treat low-cell-count/high-sparsity settings as higher-risk for overconfident interpretations. Robustness “failures” are actionable early warnings rather than defects to hide, and they give reviewers and downstream users a concrete place to look when conclusions differ between studies that nominally used the same method.

### Operational Feasibility Is a Decision Dimension

Runtime and failure behavior are part of scientific reliability. Pipelines fail not only because the biology is hard but also because dependencies, resource constraints, and long runtimes introduce brittle points. The asymmetry observed here is large enough to matter for a core facility or a benchmark paper planning many runs: RCTD succeeded in 5 of 6 attempts with a median runtime of 1853.2 s, while cell2location succeeded in 4 of 16 attempts with failures dominated by runtime errors (11/12) and one missing-dependency event. The lower cell2location success rate reflects dependency and runtime sensitivity rather than biological inferiority; successful runs concentrated later in the run log as the environment stabilized, and GPU-tier methods are gated explicitly. Reporting this operational surface is what allows a user to separate “the method disagrees with another method” from “the method failed to run here”.

### The Verification Layer Plays a Supporting Role

Byte-identity on the declared minimal pipeline shows that the framework does not inject environment noise into the artifacts it produces on that pipeline, which strengthens the discordance and robustness findings above. It does not prove byte-identity for every module, and the archive of 90 run records is a traceability record rather than a determinism proof; both limits are stated in the Results. Verification therefore supports the conclusions rather than standing as the headline: it answers the objection “is this just your pipeline noise?” and nothing more. For review, the reproducibility package lets readers validate manuscript-linked tables and figures without rerunning the analysis; for reuse, the same packaging lets collaborators compare outputs across reruns without ambiguity about preprocessing or parameterization, and to anticipate runtime and failure risks before committing to large runs. CloudBioIntegrator is not a replacement for workflow engines, toolkit workflows, or benchmark resources; it complements them by coupling standardized dual-modality preprocessing to cross-method comparison, perturbation sensitivity, failure logging, and artifact-level provenance.

### Limitations and Threats to Validity

This work has several limitations. The spatial deconvolution evidence comes from one mouse-brain dataset; replication across tissues, organisms, and technologies remains future work. Visium comparisons relied on concordance rather than paired ground truth, so the discordance finding is descriptive rather than adjudicative. The byte-identity verification covers the declared minimal pipeline, not the full analysis graph. Robustness thresholds are pragmatic decision cutoffs, not statistically derived bounds. The 102/121 success rate pools runs with heterogeneous denominators, including smoke, benchmark, and resource-limited runs. Clinical decision support and causal claims are out of scope.

There are also important threats to validity. Dataset selection bias is a primary concern: canonical examples may not reflect real-world batch effects and annotation ambiguity. We partially mitigate this by predeclaring a replication path in the evaluation matrix, but stronger generalizability will require more anchors and negative controls. GPU non-determinism is another practical concern: for GPU-tier methods (e.g., scVI [10] and cell2location [13]), strict bitwise reproducibility may not be realistic across driver/toolchain versions, and our definition of reproducibility therefore emphasizes traceability rather than determinism for those modules. Finally, metric choice can bias conclusions: concordance-based metrics do not substitute for ground truth, and some metrics favor specific method families. We mitigate this by predeclaring metrics, reporting them in tables rather than only in plots, and presenting operational constraints alongside biological summaries. A falsifiability note applies to the discordance finding: it would be confirmed or qualified by a second spatial dataset with paired ground truth, or by a null model based on reference-label permutations, and we therefore treat the current evidence as descriptive pending such replication.

Extending the method library to multi-omics settings will introduce additional sources of variability (e.g., cross-modality alignment, missing modalities, and modality-specific batch effects) and will require more explicit interface standards and reproducibility criteria than those used for single-modality scRNA-seq and Visium workflows.

### Deployment Scenarios and Future Work

Three deployment scenarios are concrete targets for the framework: core-facility QC and standardized preprocessing, where consistent intermediate tables reduce hand-off ambiguity; benchmark-paper substrates, where the same inputs must be run across many method–dataset combinations with recorded feasibility; and regulated or audit-oriented environments, where traceability of what ran and what was reported is a requirement. For exploratory daily analysis, the frozen protocol is a deliberate constraint rather than a universal recommendation.

The evaluation matrix (Supplementary Table S2) records which task–dataset rows have implemented evidence and which remain planned; adding new datasets should not require new narrative structure, only new rows in the same evidence tables and reproducibility packages. Planned next steps include: expand replication datasets beyond the canonical 10x examples (at least one additional scRNA dataset and one additional Visium dataset); add negative controls and stress tests that probe failure modes more systematically (e.g., missing metadata, stronger batch confounding, and larger runtime envelopes); and refresh the reproducibility package after any manuscript/figure updates and rerun the fresh-VM protocol provided in the Supplementary Materials.
## Data Availability Statement

All input datasets used in this study are publicly available from 10x Genomics:
- 3k PBMC scRNA-seq: https://www.10xgenomics.com/datasets/3-k-pbm-cs-from-a-healthy-donor-1-standard-1-1-0
- 10k PBMC scRNA-seq (v3 chemistry): https://www.10xgenomics.com/datasets/10-k-pbm-cs-from-a-healthy-donor-v-3-chemistry-3-standard-3-0-0
- Visium Human Lymph Node: https://www.10xgenomics.com/datasets/human-lymph-node-1-standard-1-1-0
- Visium Mouse Brain (serial section 1): https://www.10xgenomics.com/datasets/mouse-brain-serial-section-1-sagittal-anterior-1-standard-1-1-0

Toolchain documentation used to reconstruct standard inputs is publicly available at:
- Cell Ranger: https://www.10xgenomics.com/support/software/cell-ranger/latest
- Space Ranger: https://www.10xgenomics.com/support/software/space-ranger/latest

All processed tables required to reproduce the reported quantitative findings are included in Supplementary Tables S1-S6 and Supplementary Data 1 (SHA-256-verified reproducibility package, including run manifests and provenance indices). The adjacent-system positioning matrix and empirical comparison summaries used for Tables 2-3 are included in the public repository under `results/analysis/` and can be regenerated from the released benchmark tables. Author-generated analysis code used for the reported results is included in Supplementary Data 1 for peer review. The CloudBioIntegrator public code release is archived on Zenodo (v0.2.0, DOI: 10.5281/zenodo.21822504) and released on GitHub (https://github.com/Zhenghongwei11/cloudbiointegrator, release tag v0.2.0).

## Ethics Statement

This study used only publicly available, de-identified datasets and did not involve new human participant recruitment, intervention, or animal experimentation by the authors. Institutional review board approval and informed consent were therefore not required for this work.

## Author Contributions (CRediT)

Hongwei Zheng: Conceptualization, Methodology, Software, Validation, Formal analysis, Investigation, Data curation, Visualization, Writing - original draft, Writing - review & editing, Supervision, Project administration, Funding acquisition.

## Funding

This work was supported by the Science and Technology Innovation Joint Fund Project (Grant No. 2025Y9475) and the National Natural Science Foundation of China (Grant No. 82502151).

## Competing Interests

The authors declare that no competing interests exist.

## Acknowledgments

We thank the maintainers of the open-source single-cell and spatial transcriptomics tools and public datasets used in this work.

## Figures

- F1: System overview and standardized method library
- F2: Verification of reported artifacts across reruns/environments
- F3: Interoperability fidelity across scRNA and Visium module outputs
- F4: Cross-method discordance in spatial mapping
- F5: Reliability trade-offs at operational scale
- F6: Perturbation sensitivity across predeclared perturbations

### Figure captions (required for submission-ready)

**Figure 1 (F1): Technical roadmap and standardized method library.** Schematic of CloudBioIntegrator showing how multi-modal inputs (scRNA-seq and Visium matrices) flow through a shared preprocessing core with predeclared parameters, then into a modular method library with parallel scRNA-seq and Visium analysis lanes. Outputs are summarized via a unified evaluation matrix (stability, comparability, runtime/failure, robustness) and reported as benchmark summaries, spatial maps with uncertainty, and key quantitative findings.

**Figure 2 (F2): Verification of reported artifacts across reruns/environments.** Summary of run-level reproducibility records (n=90 checks) by pipeline stage and dataset, with recorded SHA-256 digests for declared output tables. Byte-level stability was additionally verified for run-specific output artifacts of the declared minimal pipeline across three independent environments, including a public cloud Linux container (manifest archived with the released reproducibility package). This evidence supports the discordance and robustness findings in Figures 4 and 6 by showing that the declared path does not inject environment noise; it is not a determinism proof for every analysis module.

**Figure 3 (F3): Interoperability fidelity across scRNA and Visium module outputs.** Benchmark summaries of scRNA QC, clustering, annotation, and integration outcomes across workflow modules, plus a Visium deconvolution interoperability panel from the same standardized intermediate-table interface. Baselines include Scanpy [7] and Seurat v5 [8]; integration methods include Harmony [9] and (where available) scVI [10]. Cohort sizes are reported alongside each summary metric, and all underlying tables are included in Supplementary Data 1.

**Figure 4 (F4): Cross-method discordance in spatial mapping.** Spatial maps for a mouse brain Visium dataset (n=2,695 spots) using a tissue-matched Allen cortex scRNA reference with provided labels [52]. Deconvolution/mapping weights are visualized for RCTD [11] and Tangram [12], with optional cell2location [13] shown when available. Uncertainty proxies (entropy of normalized weights; maximum weight) are reported to support uncertainty-aware interpretation. Cross-method agreement is quantified as spotwise cosine similarity between normalized composition vectors (median 0.364 [IQR 0.234–0.495] for RCTD vs Tangram; Supplementary Data 1). The most discordant spot (barcode ACGTGACAAAGTAAGT-1; cosine 0.006) is detailed with raw-count evidence (Supplementary Tables S5-S6), making method-dependent differences explicit under unified preprocessing.

**Figure 5 (F5): Reliability trade-offs at operational scale.** Per-run operational metrics for all recorded runs that reached a terminal outcome (n=121 runs). The run success rate is 0.843 (Wilson 95% CI: 0.768–0.897; summary in Supplementary Table S1; full row-level log in Supplementary Table S3). Metrics include wall time and recorded failure modes, enabling comparison of time-to-result and reliability across workflow modules.

**Figure 6 (F6): Perturbation sensitivity across predeclared perturbations.** Robustness outcomes for predeclared perturbations across datasets and methods. Each cell reports a delta versus nominal configuration and whether the predefined robustness threshold was met (ARI>=0.90 for seed/HVG perturbations; mean Pearson>=0.90 for Tangram HVG/2 concordance). Full perturbation definitions and the complete row-level matrix are provided in Supplementary Table S3.

## References
1. 10x Genomics. Cell Ranger (software documentation). Available from: https://www.10xgenomics.com/support/software/cell-ranger/latest Accessed 2026 Mar 1.
2. 10x Genomics. Space Ranger (software documentation). Available from: https://www.10xgenomics.com/support/software/space-ranger/latest Accessed 2026 Mar 1.
3. 10x Genomics. 3k PBMCs from a Healthy Donor. Public dataset. Available from: https://www.10xgenomics.com/datasets/3-k-pbm-cs-from-a-healthy-donor-1-standard-1-1-0 Accessed 2026 Mar 1.
4. 10x Genomics. 10k PBMCs from a Healthy Donor (v3 chemistry). Public dataset. Available from: https://www.10xgenomics.com/datasets/10-k-pbm-cs-from-a-healthy-donor-v-3-chemistry-3-standard-3-0-0 Accessed 2026 Mar 1.
5. 10x Genomics. Visium Spatial Gene Expression: Human Lymph Node. Public dataset. Available from: https://www.10xgenomics.com/datasets/human-lymph-node-1-standard-1-1-0 Accessed 2026 Mar 1.
6. 10x Genomics. Visium Spatial Gene Expression: Mouse Brain Serial Section 1 (Sagittal-Anterior). Public dataset. Available from: https://www.10xgenomics.com/datasets/mouse-brain-serial-section-1-sagittal-anterior-1-standard-1-1-0 Accessed 2026 Mar 1.
7. Wolf FA, Angerer P, Theis FJ. SCANPY: large-scale single-cell gene expression data analysis. Genome Biol. 2018;19(1):15. doi:10.1186/s13059-017-1382-0. Available from: https://doi.org/10.1186/s13059-017-1382-0
8. Hao Y, Stuart T, Kowalski MH, Choudhary S, Hoffman P, Hartman A, et al. Dictionary learning for integrative, multimodal and scalable single-cell analysis. Nat Biotechnol. 2024;42(2):293-304. doi:10.1038/s41587-023-01767-y. Available from: https://doi.org/10.1038/s41587-023-01767-y
9. Korsunsky I, Millard N, Fan J, Slowikowski K, Zhang F, Wei K, et al. Fast, sensitive and accurate integration of single-cell data with Harmony. Nat Methods. 2019;16(12):1289-1296. doi:10.1038/s41592-019-0619-0. Available from: https://doi.org/10.1038/s41592-019-0619-0
10. Lopez R, Regier J, Cole MB, Jordan MI, Yosef N. Deep generative modeling for single-cell transcriptomics. Nat Methods. 2018;15(12):1053-1058. doi:10.1038/s41592-018-0229-2. Available from: https://doi.org/10.1038/s41592-018-0229-2
11. Cable DM, Murray E, Zou LS, Goeva A, Macosko EZ, Chen F, et al. Robust decomposition of cell type mixtures in spatial transcriptomics. Nat Biotechnol. 2022;40(4):517-526. doi:10.1038/s41587-021-00830-w. Available from: https://doi.org/10.1038/s41587-021-00830-w
12. Biancalani T, Scalia G, Buffoni L, Avasthi R, Lu Z, Sanger A, et al. Deep learning and alignment of spatially resolved single-cell transcriptomes with Tangram. Nat Methods. 2021;18(11):1352-1362. doi:10.1038/s41592-021-01264-7. Available from: https://doi.org/10.1038/s41592-021-01264-7
13. Kleshchevnikov V, Shmatko A, Dann E, Aivazidis A, King HW, Li T, et al. Cell2location maps fine-grained cell types in spatial transcriptomics. Nat Biotechnol. 2022;40(5):661-671. doi:10.1038/s41587-021-01139-4. Available from: https://doi.org/10.1038/s41587-021-01139-4
14. Mölder F, Jablonski KP, Letcher B, Hall MB, van Dyken PC, Tomkins-Tinch CH, et al. Sustainable data analysis with Snakemake. F1000Res. 2021;10:33. doi:10.12688/f1000research.29032.3. Available from: https://doi.org/10.12688/f1000research.29032.3
15. Grüning B, Dale R, Sjödin A, Chapman BA, Rowe J, Tomkins-Tinch CH, et al. Bioconda: sustainable and comprehensive software distribution for the life sciences. Nat Methods. 2018;15(7):475-476. doi:10.1038/s41592-018-0046-7. Available from: https://doi.org/10.1038/s41592-018-0046-7
16. da Veiga Leprevost F, Grüning BA, Alves Aflitos S, Röst HL, Uszkoreit J, Barsnes H, et al. BioContainers: an open-source and community-driven framework for software standardization. Bioinformatics. 2017;33(16):2580-2582. doi:10.1093/bioinformatics/btx192. Available from: https://doi.org/10.1093/bioinformatics/btx192
17. O'Connor BD, Yuen D, Chung V, Duncan AG, Liu XK, Patricia J, et al. The Dockstore: enabling modular, community-focused sharing of Docker-based genomics tools and workflows. F1000Res. 2017;6:52. doi:10.12688/f1000research.10137.1. Available from: https://doi.org/10.12688/f1000research.10137.1
18. Kurtzer GM, Sochat V, Bauer MW. Singularity: Scientific containers for mobility of compute. PLoS One. 2017;12(5):e0177459. doi:10.1371/journal.pone.0177459. Available from: https://doi.org/10.1371/journal.pone.0177459
19. Afgan E, Baker D, van den Beek M, Blankenberg D, Bouvier D, Čech M, et al. The Galaxy platform for accessible, reproducible and collaborative biomedical analyses: 2016 update. Nucleic Acids Res. 2016;44(W1):W3-W10. doi:10.1093/nar/gkw343. Available from: https://doi.org/10.1093/nar/gkw343
20. Afgan E, Baker D, Batut B, van den Beek M, Bouvier D, Cech M, et al. The Galaxy platform for accessible, reproducible and collaborative biomedical analyses: 2018 update. Nucleic Acids Res. 2018;46(W1):W537-W544. doi:10.1093/nar/gky379. Available from: https://doi.org/10.1093/nar/gky379
21. The Galaxy Community. The Galaxy platform for accessible, reproducible and collaborative biomedical analyses: 2022 update. Nucleic Acids Res. 2022;50(W1):W345-W351. doi:10.1093/nar/gkac247. Available from: https://doi.org/10.1093/nar/gkac247
22. Suetake H, Tanjo T, Ishii M, P Kinoshita B, Fujino T, Hachiya T, et al. Sapporo: A workflow execution service that encourages the reuse of workflows in various languages in bioinformatics. F1000Res. 2022;11:889. doi:10.12688/f1000research.122924.2. Available from: https://doi.org/10.12688/f1000research.122924.2
23. Ewels PA, Peltzer A, Fillinger S, Patel H, Alneberg J, Wilm A, et al. The nf-core framework for community-curated bioinformatics pipelines. Nat Biotechnol. 2020;38(3):276-278. doi:10.1038/s41587-020-0439-x. Available from: https://doi.org/10.1038/s41587-020-0439-x
24. Li B, Gould J, Yang Y, Sarkizova S, Tabaka M, Ashenberg O, et al. Cumulus provides cloud-based data analysis for large-scale single-cell and single-nucleus RNA-seq. Nat Methods. 2020;17(8):793-798. doi:10.1038/s41592-020-0905-x. Available from: https://doi.org/10.1038/s41592-020-0905-x
25. Wilkinson MD, Dumontier M, Aalbersberg IJ, Appleton G, Axton M, Baak A, et al. The FAIR Guiding Principles for scientific data management and stewardship. Sci Data. 2016;3:160018. doi:10.1038/sdata.2016.18. Available from: https://doi.org/10.1038/sdata.2016.18
26. Sandve GK, Nekrutenko A, Taylor J, Hovig E. Ten simple rules for reproducible computational research. PLoS Comput Biol. 2013;9(10):e1003285. doi:10.1371/journal.pcbi.1003285. Available from: https://doi.org/10.1371/journal.pcbi.1003285
27. Wilson G, Aruliah DA, Brown CT, Chue Hong NP, Davis M, Guy RT, et al. Best practices for scientific computing. PLoS Biol. 2014;12(1):e1001745. doi:10.1371/journal.pbio.1001745. Available from: https://doi.org/10.1371/journal.pbio.1001745
28. Wilson G, Bryan J, Cranston K, Kitzes J, Nederbragt L, Teal TK. Good enough practices in scientific computing. PLoS Comput Biol. 2017;13(6):e1005510. doi:10.1371/journal.pcbi.1005510. Available from: https://doi.org/10.1371/journal.pcbi.1005510
29. List M, Ebert P, Albrecht F. Ten Simple Rules for Developing Usable Software in Computational Biology. PLoS Comput Biol. 2017;13(1):e1005265. doi:10.1371/journal.pcbi.1005265. Available from: https://doi.org/10.1371/journal.pcbi.1005265
30. Perez-Riverol Y, Gatto L, Wang R, Sachsenberg T, Uszkoreit J, Leprevost Fda V, et al. Ten Simple Rules for Taking Advantage of Git and GitHub. PLoS Comput Biol. 2016;12(7):e1004947. doi:10.1371/journal.pcbi.1004947. Available from: https://doi.org/10.1371/journal.pcbi.1004947
31. Khan FZ, Soiland-Reyes S, Sinnott RO, Lonie A, Goble C, Crusoe MR. Sharing interoperable workflow provenance: A review of best practices and their practical application in CWLProv. Gigascience. 2019. doi:10.1093/gigascience/giz095. Available from: https://doi.org/10.1093/gigascience/giz095
32. Leo S, Crusoe MR, Rodríguez-Navas L, Sirvent R, Kanitz A, De Geest P, et al. Recording provenance of workflow runs with RO-Crate. PLoS One. 2024;19(9):e0309210. doi:10.1371/journal.pone.0309210. Available from: https://doi.org/10.1371/journal.pone.0309210
33. Amezquita RA, Lun ATL, Becht E, Carey VJ, Carpp LN, Geistlinger L, et al. Orchestrating single-cell analysis with Bioconductor. Nat Methods. 2020;17(2):137-145. doi:10.1038/s41592-019-0654-x. Available from: https://doi.org/10.1038/s41592-019-0654-x
34. Luecken MD, Büttner M, Chaichoompu K, Danese A, Interlandi M, Mueller MF, et al. Benchmarking atlas-level data integration in single-cell genomics. Nat Methods. 2022;19(1):41-50. doi:10.1038/s41592-021-01336-8. Available from: https://doi.org/10.1038/s41592-021-01336-8
35. Palla G, Spitzer H, Klein M, Fischer D, Schaar AC, Kuemmerle LB, et al. Squidpy: a scalable framework for spatial omics analysis. Nat Methods. 2022;19(2):171-178. doi:10.1038/s41592-021-01358-2. Available from: https://doi.org/10.1038/s41592-021-01358-2
36. Salari A, Kiar G, Lewis L, Evans AC, Glatard T. File-based localization of numerical perturbations in data analysis pipelines. Gigascience. 2020. doi:10.1093/gigascience/giaa106. Available from: https://doi.org/10.1093/gigascience/giaa106
37. Fischer S, Crow M, Harris BD, Gillis J. Scaling up reproducible research for single-cell transcriptomics using MetaNeighbor. Nat Protoc. 2021;16(8):4031-4067. doi:10.1038/s41596-021-00575-5. Available from: https://doi.org/10.1038/s41596-021-00575-5
38. Zhao E, Stone MR, Ren X, Guenthoer J, Smythe KS, Pulliam T, et al. Spatial transcriptomics at subspot resolution with BayesSpace. Nat Biotechnol. 2021;39(11):1375-1384. doi:10.1038/s41587-021-00935-2. Available from: https://doi.org/10.1038/s41587-021-00935-2
39. Elosua-Bayes M, Nieto P, Mereu E, Gut I, Heyn H. SPOTlight: seeded NMF regression to deconvolute spatial transcriptomics spots with single-cell transcriptomes. Nucleic Acids Res. 2021;49(9):e50. doi:10.1093/nar/gkab043. Available from: https://doi.org/10.1093/nar/gkab043
40. Ma Y, Zhou X. Spatially informed cell-type deconvolution for spatial transcriptomics. Nat Biotechnol. 2022;40(9):1349-1359. doi:10.1038/s41587-022-01273-7. Available from: https://doi.org/10.1038/s41587-022-01273-7
41. Dries R, Zhu Q, Dong R, Eng CL, Li H, Liu K, et al. Giotto: a toolbox for integrative analysis and visualization of spatial expression data. Genome Biol. 2021;22(1):78. doi:10.1186/s13059-021-02286-2. Available from: https://doi.org/10.1186/s13059-021-02286-2
42. Chen J, Liu W, Luo T, Yu Z, Jiang M, Wen J, et al. A comprehensive comparison on cell-type composition inference for spatial transcriptomics data. Brief Bioinform. 2022;23(5):bbac245. doi:10.1093/bib/bbac245. Available from: https://doi.org/10.1093/bib/bbac245
43. Li H, Zhou J, Li Z, Chen S, Liao X, Zhang B, et al. A comprehensive benchmarking with practical guidelines for cellular deconvolution of spatial transcriptomics. Nat Commun. 2023;14(1):1548. doi:10.1038/s41467-023-37168-7. Available from: https://doi.org/10.1038/s41467-023-37168-7
44. Domínguez Conde C, Xu C, Jarvis LB, Rainbow DB, Wells SB, Gomes T, et al. Cross-tissue immune cell analysis reveals tissue-specific features in humans. Science. 2022;376(6594):eabl5197. doi:10.1126/science.abl5197. Available from: https://doi.org/10.1126/science.abl5197
45. Traag VA, Waltman L, van Eck NJ. From Louvain to Leiden: guaranteeing well-connected communities. Sci Rep. 2019;9(1):5233. doi:10.1038/s41598-019-41695-z. Available from: https://doi.org/10.1038/s41598-019-41695-z
46. McInnes L, Healy J, Melville J. UMAP: Uniform Manifold Approximation and Projection for Dimension Reduction. arXiv (2018). Available from: https://arxiv.org/abs/1802.03426
47. Lun AT, Bach K, Marioni JC. Pooling across cells to normalize single-cell RNA sequencing data with many zero counts. Genome Biol. 2016;17:75. doi:10.1186/s13059-016-0947-7. Available from: https://doi.org/10.1186/s13059-016-0947-7
48. Lun ATL, Riesenfeld S, Andrews T, Dao TP, Gomes T, participants in the 1st Human Cell Atlas Jamboree, Marioni JC, et al. EmptyDrops: distinguishing cells from empty droplets in droplet-based single-cell RNA sequencing data. Genome Biol. 2019;20(1):63. doi:10.1186/s13059-019-1662-y. Available from: https://doi.org/10.1186/s13059-019-1662-y
49. Wolock SL, Lopez R, Klein AM. Scrublet: Computational Identification of Cell Doublets in Single-Cell Transcriptomic Data. Cell Syst. 2019;8(4):281-291.e9. doi:10.1016/j.cels.2018.11.005. Available from: https://doi.org/10.1016/j.cels.2018.11.005
50. Hafemeister C, Satija R. Normalization and variance stabilization of single-cell RNA-seq data using regularized negative binomial regression. Genome Biol. 2019;20(1):296. doi:10.1186/s13059-019-1874-1. Available from: https://doi.org/10.1186/s13059-019-1874-1
51. Hie B, Bryson B, Berger B. Efficient integration of heterogeneous single-cell transcriptomes using Scanorama. Nat Biotechnol. 2019;37(6):685-691. doi:10.1038/s41587-019-0113-3. Available from: https://doi.org/10.1038/s41587-019-0113-3
52. Tasic B, Menon V, Nguyen TN, Kim TK, Jarsky T, Yao Z, et al. Adult mouse cortical cell taxonomy revealed by single cell transcriptomics. Nat Neurosci. 2016;19(2):335-46. doi:10.1038/nn.4216. Available from: https://doi.org/10.1038/nn.4216
53. Tasic B, Yao Z, Graybuck LT, Smith KA, Nguyen TN, Bertagnolli D, et al. Shared and distinct transcriptomic cell types across neocortical areas. Nature. 2018;563(7729):72-78. doi:10.1038/s41586-018-0654-5. Available from: https://doi.org/10.1038/s41586-018-0654-5
54. Vanlandewijck M, He L, Mäe MA, Andrae J, Ando K, Del Gaudio F, et al. A molecular atlas of cell types and zonation in the brain vasculature. Nature. 2018;554(7693):475-480. doi:10.1038/nature25739. Available from: https://doi.org/10.1038/nature25739
55. Stuart T, Butler A, Hoffman P, Hafemeister C, Papalexi E, Mauck WM 3rd, Hao Y, Stoeckius M, Smibert P, Satija R. Comprehensive integration of single-cell data. Cell. 2019;177(7):1888-1902.e21. doi:10.1016/j.cell.2019.05.031. Available from: https://doi.org/10.1016/j.cell.2019.05.031
