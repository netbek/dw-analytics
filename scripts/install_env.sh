#!/bin/bash
set -e

scripts_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
root_dir="${scripts_dir}/.."

source "${scripts_dir}/variables.sh"
source "${scripts_dir}/functions.sh"

args=("$@")
force=false

while [[ $# -gt 0 ]]; do
    case "$1" in
        -f|--force)
            force=true
            ;;
    esac
    shift
done

cd "${root_dir}"

template_env_dir="template_env"
env_dir=".env_files"
infrastructure_env_dir="infrastructure/.env_files"

mkdir -p "${env_dir}"

templates=(
    ${template_env_dir}/docker-compose.env      .env
    ${template_env_dir}/database.env            ${env_dir}/database.env
)

for ((i = 1; i < ${#templates[@]}; i+=2)); do
    template_file="${templates[i-1]}"
    output_file="${templates[i]}"

    if [ -f "${output_file}" ] && [ "$force" == false ]; then
        echo "Skipped ${template_file} because ${output_file} exists"
    else
        cp "${template_file}" "${output_file}"

        if [ -f "${output_file}" ]; then
            echo "${tput_green}Created ${output_file}${tput_reset}"
        fi
    fi
done

./infrastructure/scripts/install_env.sh --quiet "${args[@]}"

for input_file in "${infrastructure_env_dir}"/*; do
    if [ -f "${input_file}" ] && [[ ! "${input_file}" == "${infrastructure_env_dir}/."* ]]; then
        output_file="${env_dir}/$(basename "${input_file}")"

        if [ -f "${output_file}" ] && [ "$force" == false ]; then
            echo "Skipped ${input_file} because ${output_file} exists"
        else
            cp "${input_file}" "${output_file}"

            if [ -f "${output_file}" ]; then
                echo "${tput_green}Created ${output_file}${tput_reset}"
            fi
        fi
    fi
done
