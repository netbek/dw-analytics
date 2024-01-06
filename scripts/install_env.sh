#!/bin/bash
set -e

scripts_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
root_dir="${scripts_dir}/.."

source "${scripts_dir}/variables.sh"
source "${scripts_dir}/functions.sh"

function echo_help() {
    echo "Usage: $0 PROFILE [--cache/--no-cache] [-f|--force] [--prefect_postgres_default_username <value>] [--prefect_postgres_default_password <value>] [--prefect_postgres_default_database <value>] [--prefect_postgres_prefect_username <value>] [--prefect_postgres_prefect_password <value>] [--prefect_postgres_prefect_database <value>]"
    echo ""
    echo "Arguments:"
    echo "  profile: ${profile_choices[@]}"
}

if [ -z "$1" ]; then
    echo_help
    exit 1
fi

cd "${root_dir}"

template_env_dir="./template_env"
env_dir="./.env_files"
cache_file="${env_dir}/.cache.env"

mkdir -p "${env_dir}"

args=("$@")
profile="$1"
cache=true
force=false
quiet=false

declare -a variable_names=( \
    "prefect_postgres_default_username" \
    "prefect_postgres_default_password" \
    "prefect_postgres_default_database" \
    "prefect_postgres_prefect_username" \
    "prefect_postgres_prefect_password" \
    "prefect_postgres_prefect_database"
)

# Load variables from cache created in previous execution
if [ -f "${cache_file}" ] && [[ ! "${@}" =~ "--no-cache" ]]; then
    source "${cache_file}"
fi

if [[ ! " ${profile_choices[@]} " =~ " ${profile} " ]]; then
    echo "${tput_red}Invalid profile. Allowed values are: ${profile_choices[@]}${tput_reset}"
    exit 1
fi

while [[ $# -gt 0 ]]; do
    case "$1" in
        --cache)
            cache=true
            shift
            ;;
        --no-cache)
            cache=false
            shift
            ;;
        -f|--force)
            force=true
            shift
            ;;
        --quiet)
            quiet=true
            shift
            ;;
        --help)
            echo_help
            exit 0
            ;;
        --prefect_postgres_default_username)
            prefect_postgres_default_username=$2
            shift 2
            ;;
        --prefect_postgres_default_password)
            prefect_postgres_default_password=$2
            shift 2
            ;;
        --prefect_postgres_default_database)
            prefect_postgres_default_database=$2
            shift 2
            ;;
        --prefect_postgres_prefect_username)
            prefect_postgres_prefect_username=$2
            shift 2
            ;;
        --prefect_postgres_prefect_password)
            prefect_postgres_prefect_password=$2
            shift 2
            ;;
        --prefect_postgres_prefect_database)
            prefect_postgres_prefect_database=$2
            shift 2
            ;;
        *)
            shift
            ;;
    esac
done

# Check whether the required values have been given
is_complete=true
for name in "${variable_names[@]}"; do
    if [ -z "${!name}" ]; then
        is_complete=false
    fi
done

# Prompt the user to give the missing values
if [ "$is_complete" == false ]; then
    echo "Please provide the following information:"

    for name in "${variable_names[@]}"; do
        if [ -z "${!name}" ]; then
            read -p "${name}: " "$name"
        fi
    done
fi

# Save variables to be used as defaults in next execution of this script
> "${cache_file}"
for name in "${variable_names[@]}"; do
    value="${!name}"
    echo "${name}=\"${value}\"" >> "${cache_file}"
done

# Render Compose file
# mapfile -t context < <(grep -v '^#' "${template_env_dir}/docker-compose.env")
# context+=("profile=${profile}")
# render_template --pretty --prettier_parser yaml ./template_env docker-compose.yml.jinja2 "${context[@]}" > docker-compose.yml

# Render .env files
templates=(
    env.jinja2                                ./.env
    env_files/api.env.jinja2                  ${env_dir}/api.env
    env_files/cli.env.jinja2                  ${env_dir}/cli.env
    env_files/database.env.jinja2             ${env_dir}/database.env
    env_files/jupyter.env.jinja2              ${env_dir}/jupyter.env
    env_files/prefect-postgres.env.jinja2     ${env_dir}/prefect-postgres.env
    env_files/prefect-server.env.jinja2       ${env_dir}/prefect-server.env
    env_files/prefect-worker.env.jinja2       ${env_dir}/prefect-worker.env
)
context=(
    "profile=${profile}"
    "prefect_postgres_default_username=${prefect_postgres_default_username}"
    "prefect_postgres_default_password=${prefect_postgres_default_password}"
    "prefect_postgres_default_database=${prefect_postgres_default_database}"
    "prefect_postgres_prefect_username=${prefect_postgres_prefect_username}"
    "prefect_postgres_prefect_password=${prefect_postgres_prefect_password}"
    "prefect_postgres_prefect_database=${prefect_postgres_prefect_database}"
)

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
