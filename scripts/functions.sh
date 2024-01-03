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
    local pretty=0
    local prettier_parser=""

    while [[ $# -gt 0 ]]; do
        case "$1" in
            --pretty)
                pretty=1
                shift
                ;;
            --prettier_parser)
                if [ -n "$2" ]; then
                    prettier_parser="$2"
                    shift
                else
                    echo "Error: --prettier_parser option requires a valid parser argument."
                    exit 1
                fi
                shift
                ;;
            *)
                break
                ;;
        esac
    done

    if [ "$#" -lt 2 ]; then
        echo "Error: Incorrect number of arguments. Please provide 'templates_dir' and 'template'."
        exit 1
    fi

    local templates_dir="$1"
    local template="$2"
    local context=()
    shift 2

    while [[ $# -gt 0 ]]; do
        context+=("$1")
        shift
    done

    docker run --rm \
        -v ${templates_dir}:/templates \
        -e TEMPLATE=${template} \
        -e PRETTY=${pretty} \
        -e PRETTIER_PARSER=${prettier_parser} \
        ghcr.io/netbek/jinja2-docker:v0.0.5 \
        "${context[@]}"
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
