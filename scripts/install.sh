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

# Install repositories
readarray repositories < <(yq_cmd -o=csv '.repositories[] | [key, .url, .branch]' install.yml)

cd "${root_dir}/docker"

for repository in "${repositories[@]}"; do
    IFS=',' read -ra values <<< "${repository}"
    name="${values[0]}"
    url="${values[1]}"
    branch="${values[2]}"

    echo "${tput_yellow}Installing '${name}' ...${tput_reset}"

    if [ -d "$name" ]; then
        cd "$name"
        git reset --hard
        git checkout "$branch"
        git pull
        # ./scripts/install.sh
        cd ..
    else
        git clone -b "$branch" "$url" "$name"
        cd "$name"
        # ./scripts/install.sh
        cd ..
    fi

    echo "${tput_green}Installed '${name}'${tput_reset}"
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
readarray service_requirements_arr < <(yq_cmd -o=csv '.services[] | [key, .requirements[]]' install.yml)

for service_requirements in "${service_requirements_arr[@]}"; do
    IFS=',' read -ra values <<< "${service_requirements}"
    service="${values[0]}"
    requirements_arr=("${values[@]:1}")
    requirements_concat=""
    output_file="./docker/dw-prefect/${service}/build/src/requirements.txt"

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

services=$(yq_cmd -o=csv '.services | keys' docker-compose.yml | tr ", " " ")

# Pull Docker images
cmd="docker compose pull ${services}"
$cmd

# Build Docker images
cmd="docker compose build ${services} --build-arg DOCKER_UID=$(id -u) --build-arg DOCKER_GID=$(id -g)"
$cmd

echo "${tput_green}Done!${tput_reset}"
