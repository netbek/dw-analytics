#!/bin/bash
set -e

scripts_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
root_dir="${scripts_dir}/.."

source "${scripts_dir}/variables.sh"
source "${scripts_dir}/functions.sh"

function test_docker_compose_plugin() {
    docker compose version &> /dev/null
    return $?
}

if ! command_exists docker || ! test_docker_compose_plugin; then
    sudo apt install -y apt-transport-https ca-certificates curl software-properties-common

    curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo -E apt-key add -
    sudo add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable"
    sudo apt update
fi

if command_exists docker ; then
    echo "${tput_green}Docker is already installed${tput_reset}"
else
    echo "${tput_yellow}Installing Docker ...${tput_reset}"
    sudo apt-cache policy docker-ce
    sudo apt install -y docker-ce
    sudo usermod -aG docker "${USER}" # Enable docker without sudo
    echo "${tput_green}Installed Docker${tput_reset}"
fi

if test_docker_compose_plugin; then
    echo "${tput_green}Docker Compose plugin is already installed${tput_reset}"
else
    echo "${tput_yellow}Installing Docker Compose plugin ...${tput_reset}"
    sudo apt install -y docker-compose-plugin
    echo "${tput_green}Installed Docker Compose plugin${tput_reset}"
fi
