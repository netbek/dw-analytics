#!/bin/bash
set -e

scripts_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
root_dir="${scripts_dir}/.."

source "${scripts_dir}/variables.sh"
source "${scripts_dir}/functions.sh"

echo_help() {
    echo "Usage: $0 <REPOSITORY_URL> <BRANCH_NAME>"
    echo ""
    echo "Arguments:"
    echo "    repository_url: Configuration repository URL (HTTPS or SSH syntax)"
    echo "    branch_name: Branch, e.g. dev, prod"
}

if ([ "$1" == "--help" ] || [ -z "$1" ] || [ -z "$2" ]); then
    echo_help
    exit 1
fi

repo_url="$1"
branch_name="$2"
deploy_dir="deploy"
gitconfig_file="${deploy_dir}/analytics/.gitconfig"

cd "${root_dir}"

if [ -d "${deploy_dir}" ]; then
    cd "${deploy_dir}"
    git fetch origin "${branch_name}"
    git checkout "${branch_name}"
    cd ..
else
    git clone "${repo_url}" --branch "${branch_name}" "${deploy_dir}"
fi

cat <<EOF > "${gitconfig_file}"
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

echo "${tput_green}Created ${gitconfig_file}${tput_reset}"
