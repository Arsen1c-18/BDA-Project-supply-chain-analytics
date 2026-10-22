#!/usr/bin/env bash
# Build the C++ Hadoop Streaming mapper and reducer.
# This script only compiles; it does not start Hadoop or submit a job.

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd -- "$SCRIPT_DIR/.." && pwd)"
cd "$ROOT_DIR"

CXX="${CXX:-g++}"
if ! command -v "$CXX" >/dev/null 2>&1; then
    echo "ERROR: C++ compiler '$CXX' was not found." >&2
    echo "Install g++ or set CXX to an available compiler." >&2
    exit 1
fi

mkdir -p cpp/bin

# Static libstdc++/libgcc keep the executables portable across the local
# NodeManager containers and any future worker nodes with a compatible glibc.
COMMON_FLAGS=(-std=c++17 -O2 -Wall -Wextra -Wpedantic)
PORTABLE_FLAGS=(-static-libstdc++ -static-libgcc)

echo "Compiler : $("$CXX" --version | sed -n '1p')"
echo "Project  : $ROOT_DIR"
echo

echo "Building cpp/mapper/delivery_status_mapper.cpp ..."
"$CXX" "${COMMON_FLAGS[@]}" "${PORTABLE_FLAGS[@]}" \
    cpp/mapper/delivery_status_mapper.cpp \
    -o cpp/bin/delivery_status_mapper

echo "Building cpp/reducer/delivery_status_reducer.cpp ..."
"$CXX" "${COMMON_FLAGS[@]}" "${PORTABLE_FLAGS[@]}" \
    cpp/reducer/delivery_status_reducer.cpp \
    -o cpp/bin/delivery_status_reducer

echo "Building cpp/mapper/shipping_performance_mapper.cpp ..."
"$CXX" "${COMMON_FLAGS[@]}" "${PORTABLE_FLAGS[@]}" \
    cpp/mapper/shipping_performance_mapper.cpp \
    -o cpp/bin/shipping_performance_mapper

echo "Building cpp/reducer/shipping_performance_reducer.cpp ..."
"$CXX" "${COMMON_FLAGS[@]}" "${PORTABLE_FLAGS[@]}" \
    cpp/reducer/shipping_performance_reducer.cpp \
    -o cpp/bin/shipping_performance_reducer

echo "Building cpp/mapper/category_demand_mapper.cpp ..."
"$CXX" "${COMMON_FLAGS[@]}" "${PORTABLE_FLAGS[@]}" \
    cpp/mapper/category_demand_mapper.cpp \
    -o cpp/bin/category_demand_mapper

echo "Building cpp/reducer/category_demand_reducer.cpp ..."
"$CXX" "${COMMON_FLAGS[@]}" "${PORTABLE_FLAGS[@]}" \
    cpp/reducer/category_demand_reducer.cpp \
    -o cpp/bin/category_demand_reducer

chmod +x \
    cpp/bin/delivery_status_mapper \
    cpp/bin/delivery_status_reducer \
    cpp/bin/shipping_performance_mapper \
    cpp/bin/shipping_performance_reducer \
    cpp/bin/category_demand_mapper \
    cpp/bin/category_demand_reducer

echo
echo "Build complete:"
ls -lh cpp/bin/delivery_status_mapper cpp/bin/delivery_status_reducer \
    cpp/bin/shipping_performance_mapper cpp/bin/shipping_performance_reducer \
    cpp/bin/category_demand_mapper cpp/bin/category_demand_reducer
