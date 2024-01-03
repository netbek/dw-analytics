#!/bin/bash
set -e

scripts_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
root_dir="${scripts_dir}/.."

source "${scripts_dir}/variables.sh"
source "${scripts_dir}/functions.sh"

name_choices=("jupyter" "prefect-server" "vscode")

if ([ "$1" == "--help" ] || [ -z "$1" ]); then
    echo "Usage: $0 NAME"
    echo ""
    echo "Options:"
    echo "  name: ${name_choices[@]}"
    exit 1
fi

name="$1"

if [[ ! "${name_choices[@]}" =~ "${name}" ]]; then
    echo "${tput_red}Invalid name '${name}'. Valid values are: ${name_choices[@]}${tput_reset}"
    exit 1
fi

cd "${root_dir}"

source .env

if [ "${name}" == "jupyter" ]; then
    open_cmd "http://localhost:${DW_JUPYTER_PORT}/nbclassic/tree"
elif [ "${name}" == "prefect-server" ]; then
    open_cmd "http://localhost:${DW_PREFECT_SERVER_PORT}"
elif [ "${name}" == "vscode" ]; then
    vscode_remote_cmd "$PWD" /home/analyst
fi
