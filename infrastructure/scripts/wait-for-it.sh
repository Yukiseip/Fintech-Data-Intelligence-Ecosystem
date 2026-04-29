#!/bin/bash
# wait-for-it.sh
# Generic TCP service healthcheck helper.
# Usage: wait-for-it.sh <host>:<port> [-t timeout] [-- command args]
# Source: https://github.com/vishnubob/wait-for-it (MIT License, simplified)

WAITFORIT_cmdname=${0##*/}
echoerr() { if [[ $WAITFORIT_QUIET -ne 1 ]]; then echo "$@" 1>&2; fi }

usage() {
    cat << USAGE >&2
Usage:
    $WAITFORIT_cmdname host:port [-t timeout] [-- command args]
    -t TIMEOUT  Timeout in seconds (default: 15)
    -q          Quiet mode
USAGE
    exit 1
}

WAITFORIT_TIMEOUT=15
WAITFORIT_QUIET=0

while getopts ":t:q" opt; do
    case "$opt" in
        t) WAITFORIT_TIMEOUT="$OPTARG" ;;
        q) WAITFORIT_QUIET=1 ;;
        *) usage ;;
    esac
done

shift $((OPTIND-1))

if [[ "$1" == "--" ]]; then shift; fi

WAITFORIT_HOST=$(echo "$1" | cut -d: -f1)
WAITFORIT_PORT=$(echo "$1" | cut -d: -f2)
shift

if [[ -z "$WAITFORIT_HOST" || -z "$WAITFORIT_PORT" ]]; then
    echoerr "Error: you need to provide a host and port to test."
    usage
fi

WAITFORIT_start_ts=$(date +%s)

while true; do
    if (echo > /dev/tcp/"$WAITFORIT_HOST"/"$WAITFORIT_PORT") >/dev/null 2>&1; then
        WAITFORIT_end_ts=$(date +%s)
        echoerr "✅ $WAITFORIT_HOST:$WAITFORIT_PORT is available after $((WAITFORIT_end_ts - WAITFORIT_start_ts)) seconds"
        break
    fi
    WAITFORIT_curr_ts=$(date +%s)
    if [[ $((WAITFORIT_curr_ts - WAITFORIT_start_ts)) -ge $WAITFORIT_TIMEOUT ]]; then
        echoerr "❌ Timeout: $WAITFORIT_HOST:$WAITFORIT_PORT not available after ${WAITFORIT_TIMEOUT}s"
        exit 1
    fi
    echoerr "⏳ Waiting for $WAITFORIT_HOST:$WAITFORIT_PORT..."
    sleep 1
done

if [[ $# -gt 0 ]]; then exec "$@"; fi
