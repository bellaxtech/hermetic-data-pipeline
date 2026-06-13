#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# docker_export.sh
# Export Docker images as tar archives for offline/air-gapped transfer.
# Also provides an import companion script for the target environment.
#
# In air-gapped environments, you cannot docker pull from registries.
# This script:
#   1. Pulls all required images from a registry (on the online machine)
#   2. Exports each image to a tar.gz archive
#   3. Generates a load script for the air-gapped target
#   4. Optionally creates a summary manifest
#
# Usage:
#   ./deploy/docker_export.sh                         # Use default image list
#   ./deploy/docker_export.sh -i images.txt            # Custom image list
#   ./deploy/docker_export.sh -o /data/docker_images   # Custom output dir
#   ./deploy/docker_export.sh --push-to /mnt/usb       # Copy to removable media
#
# Image list format (one per line, optional # comments):
#   postgres:15
#   apache/airflow:2.8.0-python3.11
#   apache/spark:3.5.0
#   python:3.11-slim
#
# Requirements:
#   - docker >= 20.10
# ---------------------------------------------------------------------------
set -euo pipefail

# ---------------------------------------------------------------------------
# Constants & Defaults
# ---------------------------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

DEFAULT_IMAGE_LIST="${SCRIPT_DIR}/docker_images.txt"
OUTPUT_DIR="/data/docker_offline_images"
TIMESTAMP="$(date +%Y%m%d_%H%M%S)"
MANIFEST_FILE="${OUTPUT_DIR}/manifest_${TIMESTAMP}.txt"
LOAD_SCRIPT="${OUTPUT_DIR}/load_images.sh"

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

info()  { echo -e "${GREEN}[INFO]${NC} $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC} $*"; }
error() { echo -e "${RED}[ERROR]${NC} $*"; }

# ---------------------------------------------------------------------------
# Functions
# ---------------------------------------------------------------------------

usage() {
    cat <<EOF
Usage: $(basename "$0") [OPTIONS]

Export Docker images as tar.gz files for air-gapped transfer.

Options:
  -i, --images FILE       File listing Docker images to export (one per line)
                          (default: ${DEFAULT_IMAGE_LIST})
  -o, --output DIR        Output directory (default: ${OUTPUT_DIR})
  --push-to DIR           Also copy to removable media / USB drive
  --skip-pull             Skip docker pull (use locally cached images only)
  --compress              Compress each image tar with gzip (default: true)
  --no-compress           Do not compress (raw .tar files)
  -v, --verbose           Verbose output
  -h, --help              Show this help message

Examples:
  # Export default image list
  ./deploy/docker_export.sh

  # Export specific images and copy to USB
  ./deploy/docker_export.sh -i my-images.txt -o /data/exports --push-to /mnt/usb

  # Use local images only (no pull)
  ./deploy/docker_export.sh --skip-pull
EOF
    exit 0
}

check_docker() {
    if ! command -v docker &>/dev/null; then
        error "docker is not installed or not in PATH"
        exit 1
    fi
    if ! docker info &>/dev/null; then
        error "docker daemon is not running or user lacks permissions"
        exit 1
    fi
}

# ---------------------------------------------------------------------------
# Argument Parsing
# ---------------------------------------------------------------------------
IMAGE_LIST="${DEFAULT_IMAGE_LIST}"
PUSH_TO_DIR=""
SKIP_PULL=false
COMPRESS=true
VERBOSE=false

while [[ $# -gt 0 ]]; do
    case "$1" in
        -i|--images)
            IMAGE_LIST="$2"
            shift 2
            ;;
        -o|--output)
            OUTPUT_DIR="$2"
            shift 2
            ;;
        --push-to)
            PUSH_TO_DIR="$2"
            shift 2
            ;;
        --skip-pull)
            SKIP_PULL=true
            shift
            ;;
        --compress)
            COMPRESS=true
            shift
            ;;
        --no-compress)
            COMPRESS=false
            shift
            ;;
        -v|--verbose)
            VERBOSE=true
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
check_docker

# If no image list, create one with commonly used pipeline images
if [[ ! -f "${IMAGE_LIST}" ]]; then
    info "Image list not found at ${IMAGE_LIST}, creating default..."
    cat > "${IMAGE_LIST}" <<'EOF'
# Hermetic Data Pipeline - Docker Images
postgres:15
apache/airflow:2.8.0-python3.11
apache/spark:3.5.0
python:3.11-slim
minio/minio:latest
nginx:alpine
EOF
    info "Created default image list: ${IMAGE_LIST}"
fi

# ---------------------------------------------------------------------------
# Read Image List
# ---------------------------------------------------------------------------
IMAGES=()
while IFS= read -r line; do
    # Strip comments and whitespace
    line="$(echo "${line}" | sed 's/#.*//' | xargs)"
    [[ -z "${line}" ]] && continue
    IMAGES+=("${line}")
done < "${IMAGE_LIST}"

if [[ ${#IMAGES[@]} -eq 0 ]]; then
    error "No images found in: ${IMAGE_LIST}"
    exit 1
fi

info "Found ${#IMAGES[@]} images to export"
if ${VERBOSE}; then
    for img in "${IMAGES[@]}"; do
        info "  - ${img}"
    done
fi

# ---------------------------------------------------------------------------
# Prepare Output Directory
# ---------------------------------------------------------------------------
mkdir -p "${OUTPUT_DIR}"
info "Output directory: ${OUTPUT_DIR}"

# ---------------------------------------------------------------------------
# Export Process
# ---------------------------------------------------------------------------
SUCCESS_COUNT=0
FAIL_COUNT=0

for IMAGE in "${IMAGES[@]}"; do
    info "Processing image: ${IMAGE}"

    # Derive a safe filename from the image tag
    SAFE_NAME="$(echo "${IMAGE}" | tr '/:' '_')"
    OUTPUT_FILE="${OUTPUT_DIR}/${SAFE_NAME}.tar"

    # Step 1: Pull (unless --skip-pull)
    if ! ${SKIP_PULL}; then
        info "  Pulling ${IMAGE}..."
        if docker pull "${IMAGE}" 2>&1 | tail -3; then
            info "  Pull successful: ${IMAGE}"
        else
            error "  Failed to pull: ${IMAGE}"
            FAIL_COUNT=$((FAIL_COUNT + 1))
            continue
        fi
    else
        # Verify image exists locally
        if ! docker image inspect "${IMAGE}" &>/dev/null; then
            error "  Image not found locally (and --skip-pull is set): ${IMAGE}"
            FAIL_COUNT=$((FAIL_COUNT + 1))
            continue
        fi
        info "  Using local image: ${IMAGE}"
    fi

    # Step 2: Save image to tar
    info "  Saving to: ${OUTPUT_FILE}"
    if docker save "${IMAGE}" -o "${OUTPUT_FILE}"; then
        info "  Saved: ${IMAGE} -> ${OUTPUT_FILE}"
    else
        error "  Failed to save: ${IMAGE}"
        FAIL_COUNT=$((FAIL_COUNT + 1))
        continue
    fi

    # Step 3: Compress (optional)
    if ${COMPRESS}; then
        info "  Compressing..."
        gzip -f "${OUTPUT_FILE}"
        OUTPUT_FILE="${OUTPUT_FILE}.gz"
        info "  Compressed: ${OUTPUT_FILE}"
    fi

    # Record file size
    FILE_SIZE=$(du -h "${OUTPUT_FILE}" | cut -f1)
    echo "${IMAGE} | ${OUTPUT_FILE} | ${FILE_SIZE}" >> "${MANIFEST_FILE}"

    SUCCESS_COUNT=$((SUCCESS_COUNT + 1))
    info "  Done: ${IMAGE} (${FILE_SIZE})"
done

# ---------------------------------------------------------------------------
# Generate Load Script
# ---------------------------------------------------------------------------
info "Generating load script: ${LOAD_SCRIPT}"

cat > "${LOAD_SCRIPT}" <<'LOADEOF'
#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# load_images.sh
# Load Docker images from tar archives exported by docker_export.sh.
# Run this on the air-gapped target machine.
#
# Usage:
#   ./load_images.sh                    # Load all .tar and .tar.gz files in this directory
#   ./load_images.sh /path/to/images    # Load from a specific directory
# ---------------------------------------------------------------------------
set -euo pipefail

SCRIPT_DIR="${1:-$(dirname "$0")}"
cd "${SCRIPT_DIR}"

echo "[INFO] Loading Docker images from: ${SCRIPT_DIR}"

LOADED=0
FAILED=0

for f in *.tar.gz *.tar; do
    [[ -f "${f}" ]] || continue
    echo "[INFO] Loading: ${f}"

    if [[ "${f}" == *.tar.gz ]]; then
        if gunzip -c "${f}" | docker load; then
            echo "[INFO] Loaded: ${f}"
            LOADED=$((LOADED + 1))
        else
            echo "[ERROR] Failed to load: ${f}"
            FAILED=$((FAILED + 1))
        fi
    else
        if docker load -i "${f}"; then
            echo "[INFO] Loaded: ${f}"
            LOADED=$((LOADED + 1))
        else
            echo "[ERROR] Failed to load: ${f}"
            FAILED=$((FAILED + 1))
        fi
    fi
done

echo "[INFO] Load complete: ${LOADED} loaded, ${FAILED} failed"

if [[ ${FAILED} -gt 0 ]]; then
    exit 1
fi
LOADEOF

chmod +x "${LOAD_SCRIPT}"
info "Load script generated: ${LOAD_SCRIPT}"

# ---------------------------------------------------------------------------
# Copy to removable media (optional)
# ---------------------------------------------------------------------------
if [[ -n "${PUSH_TO_DIR}" ]]; then
    if [[ -d "${PUSH_TO_DIR}" ]]; then
        info "Copying to: ${PUSH_TO_DIR}"
        cp -v "${LOAD_SCRIPT}" "${PUSH_TO_DIR}/"
        cp -v "${MANIFEST_FILE}" "${PUSH_TO_DIR}/"
        if ${COMPRESS}; then
            cp -v "${OUTPUT_DIR}"/*.tar.gz "${PUSH_TO_DIR}/"
        else
            cp -v "${OUTPUT_DIR}"/*.tar "${PUSH_TO_DIR}/"
        fi
        info "Copy complete to: ${PUSH_TO_DIR}"
    else
        error "Target directory does not exist: ${PUSH_TO_DIR}"
        exit 1
    fi
fi

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
info "=" 60
info "Docker Export Summary"
info "  Image list:     ${IMAGE_LIST}"
info "  Output:         ${OUTPUT_DIR}"
info "  Successful:     ${SUCCESS_COUNT}"
info "  Failed:         ${FAIL_COUNT}"
info "  Manifest:       ${MANIFEST_FILE}"
info "  Load script:    ${LOAD_SCRIPT}"
info "=" 60
info ""
info "On the air-gapped target machine:"
info "  # Copy the output directory to the target"
info "  cd ${OUTPUT_DIR}"
info "  ./load_images.sh"
info "  # OR load individual images:"
info "  docker load -i postgres_15.tar.gz"
info "=" 60
