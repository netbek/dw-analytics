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

# Create .env files
./scripts/install_env.sh "${args[@]}"

# Create .gitconfig
if [ -f ".gitconfig" ] && [ "$force" == false ]; then
    echo "Skipped .gitconfig because it exists"
else
    echo "${tput_yellow}Creating .gitconfig ...${tput_reset}"

    cat <<EOF > .gitconfig
[core]
autocrlf = input

[pull]
rebase = false

[user]
email = $(git config --get --global user.email)
name = $(git config --get --global user.name)
EOF

    echo "${tput_green}Created .gitconfig${tput_reset}"
fi

# Concatenate requirements into single file for Docker build
readarray image_requirements_arr < <(yq_cmd -o=csv '.images[] | [key, .requirements[]]' install.yml)
processed_build_contexts=()

for image_requirements in "${image_requirements_arr[@]}"; do
    IFS=',' read -ra values <<< "${image_requirements}"
    image="${values[0]}"
    requirements_arr=("${values[@]:1}")
    requirements_concat=""
    build_context="./docker/${image}"
    output_file="${build_context}/build/src/requirements.txt"

    if [ "${#requirements_arr[@]}" -eq 0 ]; then
        continue
    fi

    for requirement in "${requirements_arr[@]}"; do
        requirement=$(realpath "${requirement}")

        if [ -f "${requirement}" ]; then
            requirements_concat+="# Source: ${requirement}\n"
            requirements_concat+="$(cat "${requirement}")"
            requirements_concat+="\n\n"
        else
            echo "${tput_red}File not found: ${requirement}${tput_reset}"
        fi
    done

    echo -e "$requirements_concat" > "$output_file"
    echo "${tput_green}Created ${output_file}${tput_reset}"
done

project_name=$(basename "$PWD")

# Build Python image
build_context="./docker/python"
cd "${build_context}"
docker build -f Dockerfile -t "${project_name}-python" .
docker builder prune -f
cd "${root_dir}"

services=$(yq_cmd -o=csv '.services | keys' docker-compose.yml | tr ", " " ")

# Pull Docker images
cmd="docker compose pull ${services}"
$cmd

# Build Docker images
cmd="docker compose build ${services} --build-arg DOCKER_UID=$(id -u) --build-arg DOCKER_GID=$(id -g)"
$cmd
docker builder prune -f

echo "${tput_green}Done!${tput_reset}"
