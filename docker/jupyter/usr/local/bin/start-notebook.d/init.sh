#!/bin/bash
set -e

if ! getent group "$NB_GID" &>/dev/null; then
    groupadd "$NB_USER" --gid "$NB_GID"
fi

chown --recursive $NB_UID:$NB_GID "/home/$NB_USER"
