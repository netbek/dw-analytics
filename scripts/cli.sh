#!/bin/bash
set -e

scripts_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
root_dir="${scripts_dir}/.."

cd "${root_dir}"

docker compose exec prefect-cli python /home/analyst/package/cli $@
