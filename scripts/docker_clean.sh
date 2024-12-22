#!/bin/bash
set -e

scripts_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
root_dir="${scripts_dir}/.."

source "${scripts_dir}/variables.sh"
source "${scripts_dir}/functions.sh"

cd "${root_dir}"

read -p "Are you sure you want to delete all the data and Docker images? (y/n): " answer

if [ "$answer" != "y" ]; then
    echo "${tput_red}Aborted.${tput_reset}"
    exit 1
fi

# Stop Tilt and delete resources
tilt down

dirs=(
    "analytics"
    "clickhouse"
    "monitor"
    "peerdb"
)

for dir in "${dirs[@]}"; do
    cd "./deploy/${dir}"

    # Delete images, volumes and networks
    docker compose down -v --rmi all

    # Delete images tagged by Tilt
    services=$(yq '.services | to_entries | map(select(.value.build != null) | .key) | .[]' docker-compose.yml)
    for service in $services; do
        image_name=$(yq_cmd ".services.${service}.image // \"${service}\"" docker-compose.yml | sed 's/:.*//')
        if [ -n $image_name ]; then
            image_ids=$(docker images --format '{{.ID}}' --filter "reference=${image_name}")
            if [[ -n $image_ids ]]; then
                docker image rm -f $image_ids
            fi
        fi
    done

    # Delete build cache
    docker builder prune -f

    cd ../..
done

# Delete .env files
rm -f ./deploy/*/.env ./deploy/*/*.env ./deploy/*/.gitconfig

echo "${tput_green}Done!${tput_reset}"
