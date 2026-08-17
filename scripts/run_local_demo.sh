#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# One command that runs the whole quality pipeline locally, exactly as CI does.
#   ./scripts/run_local_demo.sh                  -> tests + checks only
#   SONAR_TOKEN=xxx ./scripts/run_local_demo.sh  -> also scans into SonarQube
# ---------------------------------------------------------------------------
set -euo pipefail
cd "$(dirname "$0")/.."

echo "==> 1/4  Installing dependencies"
python -m pip install -q -r requirements.txt

echo "==> 2/4  Running unit tests with coverage"
pytest

echo "==> 3/4  Running PySpark anti-pattern checks"
python scripts/pyspark_checks.py src notebooks > pyspark-issues.json

if [[ -z "${SONAR_TOKEN:-}" ]]; then
  echo
  echo "==> 4/4  SKIPPED (no SONAR_TOKEN set)"
  echo "    Artifacts ready for the scanner:"
  echo "      coverage.xml       - line coverage for Sonar"
  echo "      junit.xml          - test results for Sonar"
  echo "      pyspark-issues.json- PySpark findings for Sonar"
  echo
  echo "    To scan:  docker compose up -d"
  echo "              SONAR_TOKEN=<token> ./scripts/run_local_demo.sh"
  exit 0
fi

echo "==> 4/4  Scanning into SonarQube at ${SONAR_HOST_URL:-http://localhost:9000}"
docker run --rm --network host \
  -e SONAR_HOST_URL="${SONAR_HOST_URL:-http://localhost:9000}" \
  -e SONAR_TOKEN="${SONAR_TOKEN}" \
  -v "$PWD:/usr/src" \
  sonarsource/sonar-scanner-cli

echo
echo "Done. Open ${SONAR_HOST_URL:-http://localhost:9000} to see the results."
