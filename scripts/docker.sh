#!/bin/bash
set -e

scripts_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
root_dir="${scripts_dir}/.."

source "${scripts_dir}/variables.sh"
source "${scripts_dir}/functions.sh"

echo_help() {
    echo "Usage: $0 <ACTION>"
    echo ""
    echo "Arguments:"
    echo "    action: up, down"
}

up() {
    cd deploy/clickhouse
    docker compose up -d
    cd ../..

    cd deploy/peerdb
    docker compose up -d
    cd ../..

    cd deploy/analytics
    docker compose up -d prefect-postgres prefect-server prefect-worker cli api
    cd ../..
}

down() {
    cd deploy/analytics
    docker compose down prefect-postgres prefect-server prefect-worker cli api
    cd ../..

    cd deploy/peerdb
    docker compose down
    cd ../..

    cd deploy/clickhouse
    docker compose down
    cd ../..
}

if ([ "$1" == "--help" ] || [ -z "$1" ]); then
    echo_help
    exit 1
fi

cmd="$@"

cd "${root_dir}"

if command_exists "$cmd"; then
    $cmd
else
    echo "${tput_red}Error: Action must be one of: up, down${tput_reset}"
fi
