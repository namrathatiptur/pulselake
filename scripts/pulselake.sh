#!/usr/bin/env bash
#
# Start, stop, and check the whole PulseLake stack.
#
# Three long running pieces, each in its own process:
#
#   1. the ingestion loop, polling MTA and writing to DuckDB
#   2. a dbt refresh loop, rebuilding the models so the dashboard is not
#      reading a snapshot from twenty minutes ago
#   3. Streamlit, serving the dashboard
#
# They coordinate through the DuckDB file rather than through each other.
# DuckDB takes an exclusive lock, so the ingestion loop opens and closes its
# connection once per cycle and everything else waits its turn. That is why
# all three can share one file.
#
# Usage:
#   scripts/pulselake.sh start     start everything, print the dashboard URL
#   scripts/pulselake.sh stop      stop everything
#   scripts/pulselake.sh status    show what is running and how fresh data is
#   scripts/pulselake.sh logs      tail all three logs at once

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

VENV="$PROJECT_ROOT/.venv"
PYTHON="$VENV/bin/python"
DBT="$VENV/bin/dbt"
STREAMLIT="$VENV/bin/streamlit"

PID_DIR="$PROJECT_ROOT/.run"
LOG_DIR="$PROJECT_ROOT/logs"

POLL_INTERVAL="${PULSELAKE_INTERVAL:-30}"
DBT_INTERVAL="${PULSELAKE_DBT_INTERVAL:-60}"
PORT="${PULSELAKE_PORT:-8501}"

mkdir -p "$PID_DIR" "$LOG_DIR"

require_venv() {
    if [[ ! -x "$PYTHON" ]]; then
        echo "No virtual environment found at .venv"
        echo "Create one first:"
        echo "  python3 -m venv .venv && .venv/bin/pip install -r requirements.txt"
        exit 1
    fi
}

is_running() {
    local name="$1"
    local pidfile="$PID_DIR/$name.pid"
    [[ -f "$pidfile" ]] && kill -0 "$(cat "$pidfile")" 2>/dev/null
}

start_one() {
    local name="$1"; shift
    if is_running "$name"; then
        echo "  $name already running (pid $(cat "$PID_DIR/$name.pid"))"
        return
    fi
    "$@" >> "$LOG_DIR/$name.log" 2>&1 &
    echo $! > "$PID_DIR/$name.pid"
    echo "  started $name (pid $!)"
}

stop_one() {
    local name="$1"
    local pidfile="$PID_DIR/$name.pid"
    if is_running "$name"; then
        local pid
        pid="$(cat "$pidfile")"
        # SIGTERM rather than SIGKILL. The ingestion loop traps it, finishes
        # the cycle it is in, and closes DuckDB cleanly. Killing it outright
        # can leave a lock file behind.
        kill -TERM "$pid" 2>/dev/null || true
        for _ in $(seq 1 20); do
            kill -0 "$pid" 2>/dev/null || break
            sleep 0.5
        done
        kill -KILL "$pid" 2>/dev/null || true
        echo "  stopped $name"
    else
        echo "  $name was not running"
    fi
    rm -f "$pidfile"
}

# The dbt refresh loop. Kept here as a shell loop rather than a Python module
# because that is genuinely all it is, and a scheduler is step two of the
# roadmap, not step one.
dbt_refresh_loop() {
    while true; do
        # Runs in a subshell so the cd does not leak, and the exit status is
        # swallowed on purpose: a failed rebuild should log and be retried on
        # the next tick, never kill the refresh loop.
        #
        # Written as separate statements rather than `cd ... && dbt ... || true`
        # because that reads as if-then-else and is not: the `|| true` would
        # also catch a failure of the cd itself. shellcheck flags the pattern
        # (SC2015) and it is right to.
        (
            cd "$PROJECT_ROOT/transform" || exit 1
            "$DBT" build 2>&1 | grep -E "Done\.|ERROR|WARN [0-9]" || true
        )
        sleep "$DBT_INTERVAL"
    done
}

cmd_start() {
    require_venv
    echo "Starting PulseLake"

    # One synchronous ingest first, so the database and schema exist before
    # anything tries to read them.
    echo "  priming: one ingestion cycle"
    "$PYTHON" -m ingest.run_once --quiet || {
        echo "  ingestion failed. Check logs/ingest.log"
        exit 1
    }

    echo "  priming: dbt build"
    (cd transform && "$DBT" build 2>&1 | tail -2)

    start_one "ingest" "$PYTHON" -m ingest.run_loop --interval "$POLL_INTERVAL" --quiet
    start_one "dbt" bash -c "$(declare -f dbt_refresh_loop); \
        PROJECT_ROOT='$PROJECT_ROOT' DBT='$DBT' DBT_INTERVAL='$DBT_INTERVAL' dbt_refresh_loop"
    start_one "dashboard" "$STREAMLIT" run dashboard/app.py --server.port "$PORT"

    echo
    echo "PulseLake is running."
    echo "  dashboard   http://localhost:$PORT"
    echo "  ingesting   every ${POLL_INTERVAL}s"
    echo "  dbt rebuild every ${DBT_INTERVAL}s"
    echo "  logs        logs/ingest.log, logs/dbt.log, logs/dashboard.log"
    echo "  stop with   scripts/pulselake.sh stop"
}

cmd_stop() {
    echo "Stopping PulseLake"
    stop_one "dashboard"
    stop_one "dbt"
    stop_one "ingest"
}

cmd_status() {
    echo "Processes"
    for name in ingest dbt dashboard; do
        if is_running "$name"; then
            printf "  %-11s running (pid %s)\n" "$name" "$(cat "$PID_DIR/$name.pid")"
        else
            printf "  %-11s stopped\n" "$name"
        fi
    done

    if [[ -f data/pulselake.duckdb ]] && [[ -x "$PYTHON" ]]; then
        echo
        echo "Data"
        "$PYTHON" - <<'PY' || echo "  could not read the database (it may be locked mid write)"
from ingest.storage import connect_with_retry
con = connect_with_retry(read_only=True, timeout_seconds=10)
row = con.execute("""
    SELECT count(*), max(header_timestamp),
           date_diff('second', max(header_timestamp), now())
    FROM raw_feed_fetches
""").fetchone()
print(f"  snapshots     {row[0]}")
print(f"  newest        {row[1]}")
print(f"  age           {row[2]}s")
errors = con.execute("SELECT count(*) FROM ingest_errors").fetchone()[0]
print(f"  ingest errors {errors}")
con.close()
PY
    fi
}

cmd_logs() {
    tail -n 20 -f "$LOG_DIR"/ingest.log "$LOG_DIR"/dbt.log "$LOG_DIR"/dashboard.log
}

case "${1:-}" in
    start)  cmd_start ;;
    stop)   cmd_stop ;;
    status) cmd_status ;;
    logs)   cmd_logs ;;
    restart) cmd_stop; sleep 1; cmd_start ;;
    *)
        echo "Usage: scripts/pulselake.sh {start|stop|restart|status|logs}"
        exit 1
        ;;
esac
