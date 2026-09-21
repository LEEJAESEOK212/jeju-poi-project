#!/usr/bin/env bash
set -euo pipefail

AIRFLOW_HOME_DIR="${AIRFLOW_HOME:-/home/ai-010/airflow}"
DAGS_DIR="$AIRFLOW_HOME_DIR/dags"
PYTHON_BIN="/home/ai-010/.venvs/jeju-airflow/bin/python"
AIRFLOW_BIN="/home/ai-010/.venvs/jeju-airflow/bin/airflow"
PACKAGE_DIR="$(cd "$(dirname "$0")" && pwd)"
STAMP="$(date +%Y%m%d_%H%M%S)"
BACKUP="$AIRFLOW_HOME_DIR/backups/jeju_poi_policy_$STAMP"

mkdir -p "$BACKUP" "$DAGS_DIR/orchestration/dags"
[ -f "$DAGS_DIR/jeju_poi_pipeline.py" ] && cp -a "$DAGS_DIR/jeju_poi_pipeline.py" "$BACKUP/"
[ -d "$DAGS_DIR/orchestration" ] && cp -a "$DAGS_DIR/orchestration" "$BACKUP/"

cp -f "$PACKAGE_DIR"/orchestration/*.py "$DAGS_DIR/orchestration/"
cp -f "$PACKAGE_DIR/orchestration/dags/jeju_poi_pipeline.py" "$DAGS_DIR/jeju_poi_pipeline.py"

export PYTHONPATH="$DAGS_DIR:${PYTHONPATH:-}"
"$PYTHON_BIN" -m py_compile "$DAGS_DIR"/orchestration/*.py "$DAGS_DIR/jeju_poi_pipeline.py"
(
  cd "$PACKAGE_DIR"
  "$PYTHON_BIN" -m unittest test_orchestration.py test_status_sources.py
)
"$AIRFLOW_BIN" dags reserialize >/dev/null
"$AIRFLOW_BIN" dags list-import-errors
"$AIRFLOW_BIN" dags list | grep 'jeju_poi_lifecycle_weekly_v10'

echo "DEPLOY_V16_10_2_OK"
echo "backup=$BACKUP"
echo "dag=$DAGS_DIR/jeju_poi_pipeline.py"





