#!/bin/bash
set -e

scripts_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
root_dir="${scripts_dir}/.."

source "${scripts_dir}/variables.sh"

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
