#!/usr/bin/env bash
set -euo pipefail

# Usage: AIRFLOW_BIN=/path/to/airflow bash SETUP_VARIABLES.sh /path/to/workdir /path/to/publishdir
BUNDLE_DIR="$(cd "$(dirname "$0")" && pwd)"
AIRFLOW_BIN="${AIRFLOW_BIN:-airflow}"
WORK_DIR="${1:-$BUNDLE_DIR/work}"
OUTPUT_DIR="${2:-$BUNDLE_DIR/published}"

mkdir -p "$WORK_DIR" "$OUTPUT_DIR"
"$AIRFLOW_BIN" variables set JEJU_POI_CURRENT_CSV "$BUNDLE_DIR/data/jeju_poi_master_v5.csv"
"$AIRFLOW_BIN" variables set JEJU_POI_MOIS_EVIDENCE_JSONL "$BUNDLE_DIR/data/mois_evidence_v16_3.jsonl"
"$AIRFLOW_BIN" variables set JEJU_POI_KAKAO_EVIDENCE_JSONL "$BUNDLE_DIR/data/kakao_evidence_merged_v16_9.jsonl"
"$AIRFLOW_BIN" variables set JEJU_POI_WORK_DIR "$WORK_DIR"
"$AIRFLOW_BIN" variables set JEJU_POI_OUTPUT_DIR "$OUTPUT_DIR"

echo "Airflow variables configured"
