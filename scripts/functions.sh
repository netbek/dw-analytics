#!/bin/bash

scripts_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

source "${scripts_dir}/variables.sh"

function strip() {
    echo "$(sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//' <<< "$1")"
}

function is_directory_empty() {
    shopt -s nullglob
    files=("$1"/*)
    shopt -u nullglob
    if [ ${#files[@]} -eq 0 ]; then
        return 0
    else
        return 1
    fi
}

function render_template() {
    local template_file="$1"
    local output_file="$2"
    local variables=("$@")

    local is_dry_run=false
    if [ "${variables[-1]}" == "--dry-run" ]; then
        is_dry_run=true
        unset variables[-1]
    fi

    local template=$(<"$template_file")
    local i=0

    for ((i = 3; i < ${#variables[@]}; i+=2)); do
        local variable="${variables[i-1]}"
        local value="${variables[i]}"
        template="${template//\{\{ $variable \}\}/$value}"
    done

    if [ "$is_dry_run" == true ]; then
        echo "Dry run:"
        echo "$template"
    else
        echo "$template" > "$output_file"
    fi
}

# https://stackoverflow.com/a/3931779
function command_exists() {
    type "$1" &> /dev/null;
}

function start_container() {
    local service_name="$1"
    local container_name="$2"
    local quiet=${3:-0}

    # If container is paused
    if [ "$(docker ps -aq -f status=paused -f name=^${container_name}$)" ]; then
        if [ $quiet -ne 1 ]; then
            echo "${tput_yellow}Unpausing ${service_name} ...${tput_reset}"
        fi
        docker compose unpause ${service_name}
    # If container is stopped
    elif [ "$(docker ps -aq -f status=exited -f name=^${container_name}$)" ]; then
        if [ $quiet -ne 1 ]; then
            echo "${tput_yellow}Starting ${service_name} ...${tput_reset}"
        fi
        docker compose rm -f ${service_name}
        docker compose up -d ${service_name}
    # If container is not running or does not exist
    elif [ ! "$(docker ps -q -f name=^${container_name}$)" ]; then
        if [ $quiet -ne 1 ]; then
            echo "${tput_yellow}Starting ${service_name} ...${tput_reset}"
        fi
        docker compose up -d ${service_name}
    else
        if [ $quiet -ne 1 ]; then
            echo "${tput_green}${service_name} is running${tput_reset}"
        fi
    fi
}

function stop_container() {
    local service_name="$1"
    local container_name="$2"
    local quiet=${3:-0}

    # If container is stopped
    if [ "$(docker ps -aq -f status=exited -f name=^${container_name}$)" ]; then
        docker compose rm -f
    # If container is paused or running
    elif [ "$(docker ps -q -f name=^${container_name}$)" ]; then
        docker compose down
    fi
}

function open_cmd() {
    local url=$1
    local os=$(uname -s)

    if [ "$os" == "Linux" ]; then
        xdg-open "$url" 2> /dev/null
    elif [ "$os" == "Darwin" ]; then
        open "$url"
    elif [ "$os" == "CYGWIN" ] || [ "$os" == "MINGW" ]; then
        start "$url"
    else
        echo "Unsupported operating system"
        exit 1
    fi
}

function vscode_remote_cmd() {
    p=$(printf "%s" "$1" | xxd -p) && code --remote "dev-container+${p//[[:space:]]/}" "$2"
}

function yq_cmd() {
    docker run --rm -i -v "${PWD}":/workdir mikefarah/yq:4.40.4 "$@"
}
