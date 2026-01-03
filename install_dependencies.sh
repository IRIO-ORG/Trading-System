#!/bin/bash

set -e

# ==============================
# COLOR CODES
# ==============================
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
YELLOW='\033[1;33m'
# No Color
NC='\033[0m'

RAW_OS="$(uname -s)"
RAW_ARCH="$(uname -m)"

case "${RAW_OS}" in
    Linux*)     HOST_OS="linux" ;;
    Darwin*)    HOST_OS="darwin" ;;
    *)          echo -e "${RED}Error: Unsupported OS: ${RAW_OS}${NC}"; exit 1 ;;
esac
case "${RAW_ARCH}" in
    x86_64)    HOST_ARCH="amd64"; HOST_ARCH_ALT="x86_64" ;;
    arm64)     HOST_ARCH="arm64"; HOST_ARCH_ALT="arm64" ;;
    aarch64)   HOST_ARCH="arm64"; HOST_ARCH_ALT="aarch64" ;;
    *)         echo -e "${RED}Error: Unsupported Architecture: ${RAW_ARCH}${NC}"; exit 1 ;;
esac
echo -e "${BLUE}Detected System:${NC} ${GREEN}${HOST_OS}${NC} / ${GREEN}${HOST_ARCH}${NC}"

install_tool() {
    local TOOL_NAME=$1
    local DOWNLOAD_URL=$2

    echo -e "${BLUE}Starting installation for:${NC} ${YELLOW}${TOOL_NAME}${NC}"
    
    TMP_DIR=$(mktemp -d)
    
    echo -e "  -> Downloading from: ${CYAN}${DOWNLOAD_URL}${NC}"
    
    # -f: Fail silently on HTTP errors so we catch them in the 'if'
    # -L: Follow redirects
    # -o: Output file
    if curl -fL -o "${TMP_DIR}/${TOOL_NAME}" "${DOWNLOAD_URL}"; then
        echo -e "  -> ${GREEN}Download successful.${NC}"
    else
        echo -e "  -> ${RED}Error: Failed to download ${TOOL_NAME}. Check the URL.${NC}"
        rm -rf "${TMP_DIR}"
        return 1
    fi

    echo -e "  -> Installing to /usr/local/bin ${YELLOW}(requires sudo)${NC}..."
    sudo install "${TMP_DIR}/${TOOL_NAME}" /usr/local/bin/

    rm -rf "${TMP_DIR}"
    echo -e "  -> ${GREEN}${TOOL_NAME} installed successfully!${NC}"
    echo ""
}

SKAFFOLD_URL="https://storage.googleapis.com/skaffold/releases/latest/skaffold-${HOST_OS}-${HOST_ARCH}"
install_tool "skaffold" "${SKAFFOLD_URL}"

echo -e "${GREEN}All tasks completed.${NC}"
