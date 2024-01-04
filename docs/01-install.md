# Installation

## Prerequisites

1. [Docker Engine v23 or higher](https://docs.docker.com/engine/install/) and [Docker Compose v2 or higher](https://docs.docker.com/compose/install/). Follow the links for instructions, or run this script:

    ```shell
    ./scripts/install_docker.sh
    ```

2. [Postgres v13 or higher](https://www.postgresql.org/about/news/postgresql-13-released-2077/).

3. [ClickHouse v23.8 or higher](https://clickhouse.com/docs/en/whats-new/changelog#-clickhouse-release-238-lts-2023-08-31). For development, use [https://github.com/netbek/dw-clickhouse](https://github.com/netbek/dw-clickhouse).

## Development: Installation

1. Run the install script:

    ```shell
    ./scripts/install.sh
    ```

2. Start the Postgres container.

3. Start the ClickHouse container. If using [https://github.com/netbek/dw-clickhouse](https://github.com/netbek/dw-clickhouse), then [follow these instructions](https://github.com/netbek/dw-clickhouse/blob/main/README.md#usage).

4. Start the Prefect containers:

    ```shell
    docker compose up -d prefect-postgres prefect-server prefect-worker cli
    ```

    Wait for the container statuses to change to started and healthy. If you prefer to run the containers in the foreground, then omit the `-d` option.

5. Run the provision script to configure Prefect:

    ```shell
    ./scripts/cli.sh prefect provision dev
    ```

## Development: Optional extras

### VS Code

[Download VS Code](https://code.visualstudio.com/). After installing VS Code, install the [Dev Containers extension](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-containers):

```shell
code --install-extension ms-vscode-remote.remote-containers
```

### DBeaver

GUI client for Postgres, ClickHouse and other databases. To install, run:

```shell
curl https://dbeaver.io/debs/dbeaver.gpg.key | sudo apt-key add -
echo "deb https://dbeaver.io/debs/dbeaver-ce /" | sudo tee /etc/apt/sources.list.d/dbeaver.list
sudo apt update
sudo apt install dbeaver-ce
```

See the docs for [creating a connection](https://github.com/dbeaver/dbeaver/wiki/Create-Connection).

### Aliases

Add aliases for frequently used commands to `~/.bash_aliases`:

```shell
# Start Prefect in detached mode
alias adw="cd /path/to/dw-analytics && docker compose up -d prefect-postgres prefect-server prefect-worker cli"

# Stop Prefect
alias sdw="cd /path/to/dw-analytics && docker compose down"

# Start Prefect and Jupyter in detached mode, and open Jupyter
alias jdw="cd /path/to/dw-analytics && docker compose up -d prefect-postgres prefect-server prefect-worker cli jupyter && ./scripts/open.sh jupyter"

# Start Prefect in detached mode, and open VS Code
alias cdw="cd /path/to/dw-analytics && docker compose up -d prefect-postgres prefect-server prefect-worker cli && ./scripts/open.sh vscode"
```

Set `/path/to/` to the location of the repositories on your machine. If you prefer to run the containers in the foreground, then omit the `-d` option.

### Monitoring

The resource usage of the Docker containers can be monitored with [https://github.com/netbek/dw-monitor](https://github.com/netbek/dw-monitor).

## Uninstall

To delete all the data and Docker images, run:

```shell
./scripts/uninstall.sh
```
