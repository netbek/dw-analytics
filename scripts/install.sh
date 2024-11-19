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

[help]
format = man

[pull]
rebase = false

[push]
autoSetupRemote = true

[user]
email = $(git config --get --global user.email)
name = $(git config --get --global user.name)
EOF

    echo "${tput_green}Created .gitconfig${tput_reset}"
fi

services=$(yq_cmd -o=csv '.services | keys' docker-compose.yml | tr ", " " ")

# Pull Docker images
cmd="docker compose pull ${services}"
$cmd

# Build Docker images
cmd="docker compose build ${services} --build-arg DOCKER_UID=$(id -u) --build-arg DOCKER_GID=$(id -g)"
$cmd
docker builder prune -f

echo "${tput_green}Done!${tput_reset}"
