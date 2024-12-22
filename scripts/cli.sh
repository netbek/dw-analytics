#!/bin/bash
set -e

scripts_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
root_dir="${scripts_dir}/.."

cd "${root_dir}/docker/analytics"

docker compose exec cli python /home/analyst/package/cli $@
