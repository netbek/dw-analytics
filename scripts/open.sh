#!/bin/bash
set -e

scripts_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
root_dir="${scripts_dir}/.."

source "${scripts_dir}/functions.sh"

name_choices=("cadvisor" "grafana" "jupyter" "prefect-server" "prometheus" "vscode")

if ([ "$1" == "--help" ] || [ -z "$1" ]); then
    echo "Usage: $0 NAME"
    echo ""
    echo "Options:"
    echo "  name: ${name_choices[@]}"
    exit 1
fi

name="$1"

if ! is_in_array "$name" "${name_choices[@]}"; then
    echo "Error: Invalid name '$name'. Valid values are: ${name_choices[@]}"
    exit 1
fi

cd "${root_dir}"

source .env

if [ "${name}" == "cadvisor" ]; then
    open_cmd "http://localhost:${DW_CADVISOR_PORT}"
elif [ "${name}" == "grafana" ]; then
    open_cmd "http://localhost:${DW_GRAFANA_PORT}"
elif [ "${name}" == "jupyter" ]; then
    open_cmd "http://localhost:${DW_JUPYTER_PORT}/nbclassic/tree"
elif [ "${name}" == "prefect-server" ]; then
    open_cmd "http://localhost:${DW_PREFECT_SERVER_PORT}"
elif [ "${name}" == "prometheus" ]; then
    open_cmd "http://localhost:${DW_PROMETHEUS_PORT}"
elif [ "${name}" == "vscode" ]; then
    vscode_remote_cmd "$PWD" /home/analyst
fi
