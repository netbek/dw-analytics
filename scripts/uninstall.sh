#!/bin/bash
set -e

scripts_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
root_dir="${scripts_dir}/.."

source "${scripts_dir}/variables.sh"

cd "${root_dir}"

docker compose down -v --rmi local

echo "${tput_green}Done!${tput_reset}"
