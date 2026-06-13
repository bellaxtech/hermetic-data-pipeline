#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# offline_packages.sh
# Download pip packages and their dependencies for offline (air-gapped)
# deployment. Creates a portable wheelhouse directory that can be transferred
# to environments without internet access.
#
# Usage:
#   ./deploy/offline_packages.sh                            # Use default requirements
#   ./deploy/offline_packages.sh -r requirements.txt        # Custom requirements file
#   ./deploy/offline_packages.sh -p /opt/wheelhouse         # Custom output path
#   ./deploy/offline_packages.sh -p /opt/wheelhouse -o      # Also build offline install script
#
# Features:
#   - Downloads all dependencies (not just top-level packages)
#   - Supports multiple index URLs for private registries
#   - Generates a requirements.txt freeze for reproducibility
#   - Optionally generates an install.sh script for the target environment
#   - Platform-aware: can target manylinux, musllinux, or specific arch
#
# Requirements:
#   - pip >= 21.0 (for `pip download` with --platform support)
#   - python >= 3.8
# ---------------------------------------------------------------------------
set -euo pipefail

# ---------------------------------------------------------------------------
# Constants & Defaults
# ---------------------------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

DEFAULT_REQUIREMENTS="${PROJECT_DIR}/requirements.txt"
OUTPUT_DIR="/opt/wheelhouse"
TIMESTAMP="$(date +%Y%m%d_%H%M%S)"
FREEZE_FILE="${OUTPUT_DIR}/frozen_requirements_${TIMESTAMP}.txt"
INSTALL_SCRIPT="${OUTPUT_DIR}/install_offline.sh"
PYTHON_BIN="${PYTHON:-python3}"

# Color helpers
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

info()  { echo -e "${GREEN}[INFO]${NC} $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC} $*"; }
error() { echo -e "${RED}[ERROR]${NC} $*"; }

# ---------------------------------------------------------------------------
# Functions
# ---------------------------------------------------------------------------

usage() {
    cat <<EOF
Usage: $(basename "$0") [OPTIONS]

Download Python packages and their dependencies for offline deployment.

Options:
  -r, --requirements FILE   Path to requirements file (default: ${DEFAULT_REQUIREMENTS})
  -p, --output DIR          Output directory for downloaded packages (default: ${OUTPUT_DIR})
  -o, --gen-install-script  Generate an install.sh script for the target environment
  -i, --index-url URL       Custom PyPI index URL (can be specified multiple times)
  -P, --platform PLATFORM   Target platform (e.g., manylinux2014_x86_64, musllinux_1_1_x86_64)
  -v, --verbose             Verbose output
  --no-deps                 Do not download dependencies (top-level packages only)
  -h, --help                Show this help message

Examples:
  # Basic usage
  ./deploy/offline_packages.sh

  # Custom requirements and output
  ./deploy/offline_packages.sh -r requirements-prod.txt -p /data/wheelhouse

  # With install script generation
  ./deploy/offline_packages.sh -o

  # For a specific platform (e.g., air-gapped RHEL 8 x86_64)
  ./deploy/offline_packages.sh -P manylinux2014_x86_64 -o
EOF
    exit 0
}

# ---------------------------------------------------------------------------
# Argument Parsing
# ---------------------------------------------------------------------------
REQUIREMENTS_FILE="${DEFAULT_REQUIREMENTS}"
GEN_INSTALL_SCRIPT=false
VERBOSE=false
NO_DEPS=false
EXTRA_INDEX_URLS=()
PLATFORM=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        -r|--requirements)
            REQUIREMENTS_FILE="$2"
            shift 2
            ;;
        -p|--output)
            OUTPUT_DIR="$2"
            shift 2
            ;;
        -o|--gen-install-script)
            GEN_INSTALL_SCRIPT=true
            shift
            ;;
        -i|--index-url)
            EXTRA_INDEX_URLS+=("$2")
            shift 2
            ;;
        -P|--platform)
            PLATFORM="$2"
            shift 2
            ;;
        -v|--verbose)
            VERBOSE=true
            shift
            ;;
        --no-deps)
            NO_DEPS=true
            shift
            ;;
        -h|--help)
            usage
            ;;
        *)
            error "Unknown argument: $1"
            usage
            ;;
    esac
done

# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------
if [[ ! -f "${REQUIREMENTS_FILE}" ]]; then
    error "Requirements file not found: ${REQUIREMENTS_FILE}"
    info "Create one or specify a different path with -r."
    info "Default lookup path: ${DEFAULT_REQUIREMENTS}"
    exit 1
fi

# Check Python/pip availability
if ! command -v "${PYTHON_BIN}" &>/dev/null; then
    error "Python not found: ${PYTHON_BIN}"
    exit 1
fi

PIP_BIN="${PYTHON_BIN} -m pip"
if ! ${PIP_BIN} --version &>/dev/null; then
    error "pip not available for ${PYTHON_BIN}"
    exit 1
fi

# ---------------------------------------------------------------------------
# Build Index URL Arguments
# ---------------------------------------------------------------------------
INDEX_ARGS=()
if [[ ${#EXTRA_INDEX_URLS[@]} -gt 0 ]]; then
    for url in "${EXTRA_INDEX_URLS[@]}"; do
        INDEX_ARGS+=(--extra-index-url "${url}")
    done
fi

PLATFORM_ARGS=()
if [[ -n "${PLATFORM}" ]]; then
    PLATFORM_ARGS=(--platform "${PLATFORM}" --only-binary=:all:)
    info "Targeting platform: ${PLATFORM}"
fi

DEPS_ARGS=()
if ${NO_DEPS}; then
    DEPS_ARGS+=(--no-deps)
    info "Dependency download disabled (--no-deps)"
fi

VERBOSE_ARGS=()
if ${VERBOSE}; then
    VERBOSE_ARGS+=("--verbose")
fi

# ---------------------------------------------------------------------------
# Prepare Output Directory
# ---------------------------------------------------------------------------
mkdir -p "${OUTPUT_DIR}"
FREEZE_FILE="${OUTPUT_DIR}/frozen_requirements_${TIMESTAMP}.txt"

info "=" 60
info "Offline Package Download"
info "Requirements file: ${REQUIREMENTS_FILE}"
info "Output directory:  ${OUTPUT_DIR}"
info "Python:            $(${PYTHON_BIN} --version 2>&1)"
info "=" 60

# ---------------------------------------------------------------------------
# Step 1: Freeze current environment as a reference
# ---------------------------------------------------------------------------
info "Creating frozen requirements reference..."
${PIP_BIN} freeze > "${FREEZE_FILE}" 2>/dev/null || true
info "Frozen requirements saved to: ${FREEZE_FILE}"

# ---------------------------------------------------------------------------
# Step 2: Download packages and dependencies
# ---------------------------------------------------------------------------
info "Downloading packages from: ${REQUIREMENTS_FILE}"
info "Starting download ..."

${PIP_BIN} download \
    -r "${REQUIREMENTS_FILE}" \
    -d "${OUTPUT_DIR}" \
    "${INDEX_ARGS[@]}" \
    "${PLATFORM_ARGS[@]}" \
    "${DEPS_ARGS[@]}" \
    "${VERBOSE_ARGS[@]}"

info "Download complete."

# ---------------------------------------------------------------------------
# Step 3: Count and list downloaded packages
# ---------------------------------------------------------------------------
PACKAGE_COUNT=$(find "${OUTPUT_DIR}" -maxdepth 1 -name '*.whl' -o -name '*.tar.gz' -o -name '*.zip' | wc -l)
info "Downloaded ${PACKAGE_COUNT} package files to: ${OUTPUT_DIR}"

if ${VERBOSE}; then
    info "Package list:"
    find "${OUTPUT_DIR}" -maxdepth 1 \( -name '*.whl' -o -name '*.tar.gz' -o -name '*.zip' \) -exec basename {} \; | sort
fi

# ---------------------------------------------------------------------------
# Step 4: Generate offline install script (optional)
# ---------------------------------------------------------------------------
if ${GEN_INSTALL_SCRIPT}; then
    info "Generating offline install script: ${INSTALL_SCRIPT}"

    cat > "${INSTALL_SCRIPT}" <<'INSTALLEOF'
#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# install_offline.sh
# Install Python packages from an offline wheelhouse directory.
# Generated by offline_packages.sh
#
# Usage:
#   ./install_offline.sh                    # Install all packages
#   ./install_offline.sh /custom/path       # Custom wheelhouse path
#   ./install_offline.sh --no-deps          # Skip dependencies
# ---------------------------------------------------------------------------
set -euo pipefail

WHEELHOUSE="${1:-$(dirname "$0")}"

echo "[INFO] Installing from offline wheelhouse: ${WHEELHOUSE}"

if command -v pip3 &>/dev/null; then
    PIP=pip3
elif command -v pip &>/dev/null; then
    PIP=pip
else
    echo "[ERROR] pip not found"
    exit 1
fi

${PIP} install \
    --no-index \
    --find-links "${WHEELHOUSE}" \
    --no-cache-dir \
    "${WHEELHOUSE}"/*.whl

echo "[INFO] Offline installation complete."
INSTALLEOF

    chmod +x "${INSTALL_SCRIPT}"
    info "Install script generated: ${INSTALL_SCRIPT}"
fi

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
info "=" 60
info "Offline Package Download Summary"
info "  Requirements:  ${REQUIREMENTS_FILE}"
info "  Output:        ${OUTPUT_DIR}"
info "  Packages:      ${PACKAGE_COUNT}"
info "  Frozen Reqs:   ${FREEZE_FILE}"
info "=" 60
info "To transfer to an air-gapped environment:"
info "  tar -czf wheelhouse.tar.gz -C ${OUTPUT_DIR} ."
info ""
info "In the target environment:"
info "  pip install --no-index --find-links ./wheelhouse -r requirements.txt"
info "  # OR run the generated script (if -o was specified):"
info "  ./install_offline.sh ./wheelhouse"
info "=" 60
