#!/bin/bash
set -e

scripts_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
root_dir="${scripts_dir}/.."

source "${scripts_dir}/variables.sh"
source "${scripts_dir}/functions.sh"

echo_help() {
    echo "Usage: $0 PROFILE \\
    [--cache/--no-cache] \\
    [-f|--force] \\
    [--clickhouse_default_username <value>] \\
    [--clickhouse_default_password <value>] \\
    [--clickhouse_default_database <value>] \\
    [--prefect_postgres_default_username <value>] \\
    [--prefect_postgres_default_password <value>] \\
    [--prefect_postgres_default_database <value>] \\
    [--prefect_postgres_prefect_username <value>] \\
    [--prefect_postgres_prefect_password <value>] \\
    [--prefect_postgres_prefect_database <value>]"
    echo ""
    echo "Arguments:"
    echo "    profile: ${profile_choices[@]}"
}

cd "${root_dir}"

cache_dir="./.cache"
cache_file="${cache_dir}/variables.env"

declare -a variable_names=( \
    "clickhouse_default_username" \
    "clickhouse_default_password" \
    "clickhouse_default_database" \
    "prefect_postgres_default_username" \
    "prefect_postgres_default_password" \
    "prefect_postgres_default_database" \
    "prefect_postgres_prefect_username" \
    "prefect_postgres_prefect_password" \
    "prefect_postgres_prefect_database"
)

profile="$1"
cache=true
force=false
quiet=false

mkdir -p "${cache_dir}"

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
        --clickhouse_default_username)
            clickhouse_default_username=$2
            shift 2
            ;;
        --clickhouse_default_password)
            clickhouse_default_password=$2
            shift 2
            ;;
        --clickhouse_default_database)
            clickhouse_default_database=$2
            shift 2
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

# Render .env files
templates=(
    ./deploy/clickhouse/templates/env.jinja2                   ./deploy/clickhouse/.env
    ./deploy/clickhouse/templates/clickhouse.env.jinja2        ./deploy/clickhouse/clickhouse.env

    ./deploy/analytics/templates/env.jinja2                    ./deploy/analytics/.env
    ./deploy/analytics/templates/api.env.jinja2                ./deploy/analytics/api.env
    ./deploy/analytics/templates/cli.env.jinja2                ./deploy/analytics/cli.env
    ./deploy/analytics/templates/database.env.jinja2           ./deploy/analytics/database.env
    ./deploy/analytics/templates/jupyter.env.jinja2            ./deploy/analytics/jupyter.env
    ./deploy/analytics/templates/prefect-postgres.env.jinja2   ./deploy/analytics/prefect-postgres.env
    ./deploy/analytics/templates/prefect-server.env.jinja2     ./deploy/analytics/prefect-server.env
    ./deploy/analytics/templates/prefect-worker.env.jinja2     ./deploy/analytics/prefect-worker.env
    ./deploy/analytics/templates/test-postgres.env.jinja2      ./deploy/analytics/test-postgres.env
    ./deploy/analytics/templates/test-clickhouse.env.jinja2    ./deploy/analytics/test-clickhouse.env

    ./deploy/monitor/templates/env.jinja2                      ./deploy/monitor/.env
    ./deploy/monitor/templates/cadvisor.env.jinja2             ./deploy/monitor/cadvisor.env
    ./deploy/monitor/templates/grafana.env.jinja2              ./deploy/monitor/grafana.env
    ./deploy/monitor/templates/prometheus.env.jinja2           ./deploy/monitor/prometheus.env
)
context=(
    "profile=${profile}"
    "clickhouse_default_username=${clickhouse_default_username}"
    "clickhouse_default_password=${clickhouse_default_password}"
    "clickhouse_default_database=${clickhouse_default_database}"
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
        if [ "$quiet" == false ]; then
            echo "Skipped ${template_file} because ${output_file} exists"
        fi
    else
        render_template "$(dirname "${template_file}")" "$(basename "${template_file}")" "${context[@]}" > "${output_file}"

        if [ -f "${output_file}" ]; then
            if [ "$quiet" == false ]; then
                echo "${tput_green}Created ${output_file}${tput_reset}"
            fi
        fi
    fi
done

# Create ./deploy/analytics/.gitconfig
if [ -f "./deploy/analytics/.gitconfig" ] && [ "$force" == false ]; then
    echo "Skipped ./deploy/analytics/.gitconfig because it exists"
else
    cat <<EOF > ./deploy/analytics/.gitconfig
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
    echo "${tput_green}Created ./deploy/analytics/.gitconfig${tput_reset}"
fi
