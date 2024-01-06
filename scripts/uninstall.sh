#!/bin/bash
set -e

scripts_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
root_dir="${scripts_dir}/.."

source "${scripts_dir}/variables.sh"

cd "${root_dir}"

read -p "Are you sure you want to delete all the data and Docker images? (y/n): " answer

if [ "$answer" != "y" ]; then
    echo "${tput_red}Aborted.${tput_reset}"
    exit 1
fi

docker compose down -v --rmi local
docker builder prune -f

echo "${tput_green}Done!${tput_reset}"
