#!/bin/bash

command_exists() {
    command -v "$1" > /dev/null 2>&1
}

version_gte() {
    local cmd="$1"
    local required_version="$2"
    local installed_version=$($cmd | grep -oE "[0-9]+(\.[0-9]+)+" | head -n 1)

    if [ -z "$installed_version" ]; then
        return 1
    fi

    IFS="."
    set -- $installed_version
    local installed_parts="$@"
    set -- $required_version
    local required_parts="$@"
    unset IFS

    local i=1
    for required_part in $required_parts; do
        installed_part=$(echo "$installed_parts" | cut -d " " -f $i)
        if [ "${installed_part:-0}" -lt "$required_part" ]; then
            return 1
        elif [ "${installed_part:-0}" -gt "$required_part" ]; then
            return 0
        fi
        i=$((i + 1))
    done

    return 0
}

render_template() {
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

open_cmd() {
    local url=$1
    local os=$(uname -s)

    if [ "$os" == "Linux" ]; then
        xdg-open "$url" 2> /dev/null
    elif [ "$os" == "Darwin" ]; then
        open "$url"
    elif [ "$os" == "CYGWIN" ] || [ "$os" == "MINGW" ]; then
        start "$url"
    else
        echo "Error: Operating system '$os' not supported."
        exit 1
    fi
}

vscode_remote_cmd() {
    p=$(printf "%s" "$1" | xxd -p) && code --remote "dev-container+${p//[[:space:]]/}" "$2"
}

yq_cmd() {
    docker run --rm -i -v "${PWD}":/workdir mikefarah/yq:4.40.4 "$@"
}
