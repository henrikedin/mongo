#!/bin/bash
set -o verbose
set -o errexit

# This script downloads and imports jbcoe polymorphic_value.

JBCOE_GIT_URL="https://raw.githubusercontent.com/jbcoe/polymorphic_value"
JBCOE_GIT_REV=dac4a42db42597362761806ba08aec13b1420f00
JBCOE_GIT_DIR="$(git rev-parse --show-toplevel)/src/third_party/jbcoe"

mkdir -p "${JBCOE_GIT_DIR}"

wget "${JBCOE_GIT_URL}/${JBCOE_GIT_REV}/polymorphic_value.h" \
    -O "${JBCOE_GIT_DIR}/polymorphic_value.h"

wget "${JBCOE_GIT_URL}/${JBCOE_GIT_REV}/LICENSE.txt" \
    -O "${JBCOE_GIT_DIR}/LICENSE.txt"

