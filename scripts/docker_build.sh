#!/bin/bash
set -e

scripts_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
root_dir="${scripts_dir}/.."

source "${scripts_dir}/variables.sh"
source "${scripts_dir}/functions.sh"

cd "${root_dir}"

# Build and pull Docker images
dirs=(
    "analytics"
    "clickhouse"
    "monitor"
    "peerdb"
)

for dir in "${dirs[@]}"; do
    cd "./docker/${dir}"

    services=$(yq '.services | to_entries | map(select(.value.build != null) | .key) | .[]' docker-compose.yml)
    for service in $services; do
        cmd="docker compose build ${service} --build-arg DOCKER_UID=$(id -u) --build-arg DOCKER_GID=$(id -g)"
        $cmd
    done

    services=$(yq '.services | to_entries | map(select(.value.build == null) | .key) | .[]' docker-compose.yml)
    for service in $services; do
        cmd="docker compose pull ${service}"
        $cmd
    done

    cd ../..
done

echo "${tput_green}Done!${tput_reset}"
