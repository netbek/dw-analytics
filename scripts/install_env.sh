#!/bin/bash
set -e

scripts_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
root_dir="${scripts_dir}/.."

source "${scripts_dir}/variables.sh"
source "${scripts_dir}/functions.sh"

function echo_help() {
    echo "Usage: $0 PROFILE [-f|--force]"
    echo ""
    echo "Arguments:"
    echo "  profile: ${profile_choices[@]}"
}

if [ -z "$1" ]; then
    echo_help
    exit 1
fi

args=("$@")
profile="$1"
force=false

if [[ ! " ${profile_choices[@]} " =~ " ${profile} " ]]; then
    echo "${tput_red}Invalid profile. Allowed values are: ${profile_choices[@]}${tput_reset}"
    exit 1
fi

while [[ $# -gt 0 ]]; do
    case "$1" in
        -f|--force)
            force=true
            ;;
        --help)
            echo_help
            exit 0
            ;;
    esac
    shift
done

cd "${root_dir}"

template_env_dir="./template_env"
env_dir="./.env_files"

mkdir -p "${env_dir}"

# Render Compose file
# mapfile -t context < <(grep -v '^#' "${template_env_dir}/docker-compose.env")
# context+=("profile=${profile}")
# render_template --pretty --prettier_parser yaml ./template_env docker-compose.yml.jinja2 "${context[@]}" > docker-compose.yml

# Render .env files
templates=(
    env.jinja2                          ./.env
    env_files/database.env.jinja2       ${env_dir}/database.env
)
context=("profile=${profile}")

for ((i = 1; i < ${#templates[@]}; i+=2)); do
    template_file="${templates[i-1]}"
    output_file="${templates[i]}"

    if [ -f "${output_file}" ] && [ "$force" == false ]; then
        echo "Skipped ${template_file} because ${output_file} exists"
    else
        render_template "${template_env_dir}" "${template_file}" "${context[@]}" > "${output_file}"

        if [ -f "${output_file}" ]; then
            echo "${tput_green}Created ${output_file}${tput_reset}"
        fi
    fi
done

# Render .env files of Docker images
readarray repositories < <(yq_cmd -o=csv '.repositories[] | key' install.yml)

if [ "$force" == true ]; then
    filtered_args=("--force")
else
    filtered_args=()
fi

for repository in "${repositories[@]}"; do
    repository=$(strip "${repository}")
    repository_env_dir="docker/${repository}/.env_files"

    ./docker/${repository}/scripts/install_env.sh --quiet "${filtered_args[@]}"

    for input_file in "${repository_env_dir}"/*; do
        if [ -f "${input_file}" ] && [[ ! "${input_file}" == "${repository_env_dir}/."* ]]; then
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
done
