#!/usr/bin/env bash

set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

CONTAINER_NAME="ethereum_screener"
LOGS_PID=""
CLEANED_UP=0
APP_STARTED=0
INTERRUPTED=0

mapfile -t IMAGE_NAMES < <(docker compose config --images)

if [[ ${#IMAGE_NAMES[@]} -eq 0 ]]; then
    printf '[start.sh] Unable to determine Compose image names.\n' >&2
    exit 1
fi

log() {
    printf '[start.sh] %s\n' "$*"
}

remove_app_resources() {
    docker compose down --remove-orphans --rmi local >/dev/null 2>&1 || true
    docker rm -f "$CONTAINER_NAME" >/dev/null 2>&1 || true

    local image_name
    for image_name in "${IMAGE_NAMES[@]}"; do
        docker image rm -f "$image_name" >/dev/null 2>&1 || true
    done
}

verify_cleanup() {
    local cleanup_ok=0

    if docker container inspect "$CONTAINER_NAME" >/dev/null 2>&1; then
        log "Cleanup check failed: container still exists -> $CONTAINER_NAME"
        cleanup_ok=1
    else
        log "Cleanup check passed: container removed -> $CONTAINER_NAME"
    fi

    local image_name
    for image_name in "${IMAGE_NAMES[@]}"; do
        if docker image inspect "$image_name" >/dev/null 2>&1; then
            log "Cleanup check failed: image still exists -> $image_name"
            cleanup_ok=1
        else
            log "Cleanup check passed: image removed -> $image_name"
        fi
    done

    return "$cleanup_ok"
}

stop_logs_stream() {
    if [[ -z "$LOGS_PID" ]] || ! kill -0 "$LOGS_PID" >/dev/null 2>&1; then
        return 0
    fi

    local child_pids
    child_pids="$(pgrep -P "$LOGS_PID" || true)"

    if [[ -n "$child_pids" ]]; then
        kill $child_pids >/dev/null 2>&1 || true
    fi
    kill "$LOGS_PID" >/dev/null 2>&1 || true

    sleep 1

    child_pids="$(pgrep -P "$LOGS_PID" || true)"
    if [[ -n "$child_pids" ]]; then
        kill -9 $child_pids >/dev/null 2>&1 || true
    fi
    if kill -0 "$LOGS_PID" >/dev/null 2>&1; then
        kill -9 "$LOGS_PID" >/dev/null 2>&1 || true
    fi

    wait "$LOGS_PID" >/dev/null 2>&1 || true
    LOGS_PID=""
}

wait_for_logs_stream() {
    if [[ -z "$LOGS_PID" ]]; then
        return 0
    fi

    set +e
    wait "$LOGS_PID"
    local status=$?
    set -e

    LOGS_PID=""
    return "$status"
}

cleanup_and_verify() {
    if (( CLEANED_UP )); then
        return 0
    fi
    CLEANED_UP=1

    stop_logs_stream

    log "Stopping app and removing this app's container/image..."
    remove_app_resources

    log "Verifying cleanup..."
    verify_cleanup
}

on_interrupt() {
    if (( INTERRUPTED )); then
        return 0
    fi

    INTERRUPTED=1
    log "Ctrl+C received. Stopping log stream before cleanup..."
    stop_logs_stream
}

on_exit() {
    local status=$?
    trap - EXIT INT TERM

    if (( APP_STARTED )) && (( ! CLEANED_UP )); then
        log "Exit detected. Stopping app and cleaning up..."
        if ! cleanup_and_verify; then
            status=1
        fi
    fi

    exit "$status"
}

trap on_interrupt INT TERM
trap on_exit EXIT

log "Removing existing app container/image before start..."
remove_app_resources

log "Verifying pre-start cleanup..."
verify_cleanup

log "Building and starting the app..."
docker compose up --build -d
APP_STARTED=1

log "App is starting. Current Compose status:"
docker compose ps

log "Showing live logs. Press Ctrl+C to stop the app and clean up."
docker compose logs -f --tail=50 &
LOGS_PID=$!

LOGS_STATUS=0
set +e
wait_for_logs_stream
LOGS_STATUS=$?
set -e

if (( INTERRUPTED )); then
    log "Log streaming interrupted by Ctrl+C. Cleaning up app resources..."
elif (( LOGS_STATUS != 0 )); then
    log "Log streaming exited with status $LOGS_STATUS. Cleaning up app resources..."
else
    log "Log streaming ended. Cleaning up app resources..."
fi

cleanup_and_verify
APP_STARTED=0

if (( INTERRUPTED )); then
    exit 130
fi

exit "$LOGS_STATUS"
