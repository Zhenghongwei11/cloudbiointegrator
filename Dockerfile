FROM rocker/r2u:24.04

WORKDIR /app

ENV DEBIAN_FRONTEND=noninteractive
SHELL ["/bin/bash", "-lc"]

# Goal: keep docker builds fast on arm64 by avoiding source builds for Seurat.
# r2u provides prebuilt CRAN .deb packages; we install Seurat v5 via apt (no compilation).
# We then build CPython 3.11 from source to preserve the project's pinned Python stack
# (Ubuntu 24.04 ships Python 3.12 by default, but our pins include SciPy 1.11.x -> py3.11).
ARG PYTHON_VERSION=3.11.11
ARG PYTHON_TGZ_SHA256=883bddee3c92fcb91cf9c09c5343196953cbb9ced826213545849693970868ed

# Ubuntu arm64 sources in the base image use http://ports.ubuntu.com (port 80) which can be flaky/blocked.
# Switch to HTTPS to stabilize builds.
RUN sed -i "s|URIs: http://ports.ubuntu.com/ubuntu-ports/|URIs: https://ports.ubuntu.com/ubuntu-ports/|g" /etc/apt/sources.list.d/ubuntu.sources \
  && apt-get update -o Acquire::Retries=8 -o Acquire::http::Timeout=60 -o Acquire::https::Timeout=60 \
  && (apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    git \
    make \
    pkg-config \
    cmake \
    gcc \
    g++ \
    gfortran \
    libcurl4-openssl-dev \
    libssl-dev \
    libxml2-dev \
    libfontconfig1-dev \
    libfreetype6-dev \
    libpng-dev \
    libjpeg-dev \
    libtiff-dev \
    libharfbuzz-dev \
    libfribidi-dev \
    libhdf5-dev \
    libopenblas-dev \
    liblapack-dev \
    libglpk-dev \
    zlib1g-dev \
    libbz2-dev \
    libreadline-dev \
    libsqlite3-dev \
    libffi-dev \
    liblzma-dev \
    tk-dev \
    uuid-dev \
  || (apt-get update -o Acquire::Retries=8 -o Acquire::http::Timeout=60 -o Acquire::https::Timeout=60 \
    && apt-get install -y --no-install-recommends --fix-missing \
      ca-certificates curl git make pkg-config cmake gcc g++ gfortran \
      libcurl4-openssl-dev libssl-dev libxml2-dev libfontconfig1-dev libfreetype6-dev libpng-dev libjpeg-dev libtiff-dev \
      libharfbuzz-dev libfribidi-dev libhdf5-dev libopenblas-dev liblapack-dev libglpk-dev \
      zlib1g-dev libbz2-dev libreadline-dev libsqlite3-dev libffi-dev liblzma-dev tk-dev uuid-dev)) \
  && (apt-get install -y --no-install-recommends \
    r-cran-seurat=5.4.0-1.ca2404.1 \
    r-cran-leidenbase=0.1.36-1.ca2404.1 \
    r-cran-ggplot2 \
    r-cran-patchwork \
    r-cran-ggrepel \
    r-cran-stringr \
    r-cran-tidyr \
    r-cran-remotes \
    || (apt-get update -o Acquire::Retries=8 -o Acquire::http::Timeout=60 -o Acquire::https::Timeout=60 \
      && apt-get install -y --no-install-recommends --fix-missing \
        r-cran-seurat=5.4.0-1.ca2404.1 r-cran-leidenbase=0.1.36-1.ca2404.1 \
        r-cran-ggplot2 r-cran-patchwork r-cran-ggrepel r-cran-stringr r-cran-tidyr r-cran-remotes)) \
  && rm -rf /var/lib/apt/lists/*

# Build CPython 3.11 (needed for pinned SciPy stack) and make it the default python3.
RUN set -euo pipefail; \
  apt-get update -o Acquire::Retries=5; \
  curl -fsSL "https://www.python.org/ftp/python/${PYTHON_VERSION}/Python-${PYTHON_VERSION}.tgz" -o /tmp/Python.tgz; \
  echo "${PYTHON_TGZ_SHA256}  /tmp/Python.tgz" | sha256sum -c -; \
  tar -xzf /tmp/Python.tgz -C /tmp; \
  cd "/tmp/Python-${PYTHON_VERSION}"; \
  ./configure --prefix=/opt/python311 --with-ensurepip=install; \
  make -j"$(nproc)"; \
  make install; \
  echo "/opt/python311/lib" > /etc/ld.so.conf.d/python311.conf; \
  ldconfig; \
  ln -sf /opt/python311/bin/python3.11 /usr/local/bin/python3; \
  ln -sf /opt/python311/bin/pip3.11 /usr/local/bin/pip; \
  ln -sf /opt/python311/bin/pip3.11 /usr/local/bin/pip3; \
  python3 -m pip install --no-cache-dir --upgrade pip setuptools wheel; \
  rm -rf /tmp/Python.tgz "/tmp/Python-${PYTHON_VERSION}"; \
  rm -rf /var/lib/apt/lists/*

# Copy constraints early so pip install can use them (keeps later COPY layer smaller).
COPY requirements/ /app/requirements/

# Python deps
# - matplotlib: placeholder figure scripts
# - pinned numeric stack: avoid ABI churn (especially with compiled deps like scikit-misc/numba)
# - scanpy stack: scRNA method-pack runner (baseline)
RUN pip install --no-cache-dir \
    matplotlib==3.9.4 \
    numpy==1.26.4 \
    scipy==1.11.4 \
    pandas==2.2.2 \
    scikit-learn==1.4.2

RUN pip install --no-cache-dir \
    -c requirements/pip_constraints.txt \
    scanpy==1.10.3 \
    igraph==1.0.0 \
    leidenalg==0.11.0 \
    celltypist==1.7.1 \
    harmonypy==0.0.10

# Visium deconvolution/mapping deps (advanced).
#
# Torch strategy:
# - Default build uses CPU torch wheels (portable, smaller).
# - For GPU VMs, enable CUDA torch wheels with:
#     docker build --build-arg INSTALL_CUDA_TORCH=1 ...
ARG INSTALL_CUDA_TORCH=0
RUN if [[ "${INSTALL_CUDA_TORCH}" == "1" ]]; then \
      pip install --no-cache-dir --extra-index-url https://download.pytorch.org/whl/cu121 torch==2.2.2+cu121; \
    else \
      arch="$(uname -m)"; \
      if [[ "$arch" == "x86_64" ]]; then \
        pip install --no-cache-dir --index-url https://download.pytorch.org/whl/cpu torch==2.2.2+cpu; \
      else \
        pip install --no-cache-dir torch==2.2.2; \
      fi; \
    fi

RUN pip install --no-cache-dir \
    tangram-sc==1.0.4

# Optional Visium deconvolution runner: cell2location (extra deps; off by default).
# Enable with: docker build --build-arg INSTALL_CELL2LOCATION=1 .
ARG INSTALL_CELL2LOCATION=0
RUN if [[ "${INSTALL_CELL2LOCATION}" == "1" ]]; then \
      pip install --no-cache-dir \
        pyro-ppl==1.9.1 \
        scvi-tools==1.1.3 \
        jax==0.4.28 \
        jaxlib==0.4.28 \
        opencv-python==4.10.0.84 \
        cell2location==0.1.4; \
    fi

# Visium deconvolution baseline (RCTD via spacexr), pinned to a commit for reproducibility.
# Set INSTALL_SPACEXR=0 to skip in minimal images (scRNA-only smoke runs).
ARG INSTALL_SPACEXR=1
ARG SPACEXR_COMMIT=9f5dc33c8060f946c6072a138b70e189636e1435
ARG SPACEXR_TGZ_SHA256=97d50ed7d102201d324b49e5dd4494aa0a63906df76a7a269638e10210dc656d
RUN if [[ "${INSTALL_SPACEXR}" == "1" ]]; then \
    curl -fsSL --retry 8 --retry-delay 2 --retry-connrefused \
      "https://codeload.github.com/dmcable/spacexr/tar.gz/${SPACEXR_COMMIT}" -o /tmp/spacexr.tar.gz; \
    echo "${SPACEXR_TGZ_SHA256}  /tmp/spacexr.tar.gz" | sha256sum -c -; \
    R -q -e 'options(repos=c(CRAN="https://cloud.r-project.org")); \
      remotes::install_local("/tmp/spacexr.tar.gz", upgrade="never", dependencies=TRUE); \
      if (!requireNamespace("spacexr", quietly=TRUE)) stop("spacexr install failed");'; \
    rm -f /tmp/spacexr.tar.gz; \
  else \
    echo "Skipping spacexr install (INSTALL_SPACEXR=0)"; \
  fi

COPY . /app

ENV PYTHONUNBUFFERED=1
ENV HOME=/tmp
ENV XDG_CACHE_HOME=/tmp/xdg_cache
ENV MPLCONFIGDIR=/tmp/matplotlib
ENV NUMBA_CACHE_DIR=/tmp/numba_cache

# Default command prints help; actual runs use `make skeleton|smoke|validate`.
CMD ["python3", "scripts/pipeline/run.py", "--help"]
