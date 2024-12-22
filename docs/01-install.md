# Installation

## Prerequisites

1. [Postgres v13 or higher](https://www.postgresql.org/about/news/postgresql-13-released-2077/).

## Development

### Install

1. [Docker Engine v23 or higher](https://docs.docker.com/engine/install/) and [Docker Compose v2 or higher](https://docs.docker.com/compose/install/). Follow the links for instructions or run this script:

    ```shell
    ./scripts/install.sh docker
    ```

2. [Tilt v0.33.20 or higher](https://docs.tilt.dev/install). Follow the link for instructions or run this script:

    ```shell
    ./scripts/install.sh tilt
    ```

3. Clone PeerDB and create the .env files for deployment:

    ```shell
    ./scripts/install.sh peerdb
    ./scripts/docker_env.sh dev
    ```

4. Start the services:

    ```shell
    tilt up --port 29000
    ```

    Wait a few minutes for the Docker images to be built. To check the progress, open [http://localhost:29000](http://localhost:29000).

5. Run the provision script to configure Prefect:

    ```shell
    ./scripts/cli.sh prefect provision dev
    ```

### Optional extras

#### VS Code

[Download VS Code](https://code.visualstudio.com/). After installing VS Code, install the [Dev Containers extension](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-containers):

```shell
code --install-extension ms-vscode-remote.remote-containers
```

#### DBeaver

Client for Postgres, ClickHouse and other databases. To install, run:

```shell
curl https://dbeaver.io/debs/dbeaver.gpg.key | sudo apt-key add -
echo "deb https://dbeaver.io/debs/dbeaver-ce /" | sudo tee /etc/apt/sources.list.d/dbeaver.list
sudo apt update
sudo apt install dbeaver-ce
```

See the docs for [creating a connection](https://github.com/dbeaver/dbeaver/wiki/Create-Connection).

#### Aliases

Add aliases for frequently used commands to `~/.bash_aliases`:

```shell
# Connect to ClickHouse
alias cch="cd /path/to/dw-analytics/deploy/clickhouse && docker compose exec clickhouse clickhouse-client --user analyst --password analyst"

# Open Jupyter
alias jdw="cd /path/to/dw-analytics/analytics/scripts/open.sh jupyter"

# Open VS Code
alias cdw="cd /path/to/dw-analytics/analytics/scripts/open.sh vscode"
```

Set `/path/to/` to the location of the repository on your machine.

### Uninstall

To delete all the data and Docker images, run:

```shell
./scripts/docker_clean.sh
```

## Production

### Install

1. [Docker Engine v23 or higher](https://docs.docker.com/engine/install/) and [Docker Compose v2 or higher](https://docs.docker.com/compose/install/). Follow the links for instructions or run this script:

    ```shell
    ./scripts/install.sh docker
    ```

2. Clone PeerDB, create the .env files for deployment, and build the Docker images:

    ```shell
    ./scripts/install.sh peerdb
    ./scripts/docker_env.sh prod
    ./scripts/docker_build.sh
    ```

3. Start the services:

    ```shell
    cd /path/to/dw-analytics/deploy/clickhouse && docker compose up -d
    cd /path/to/dw-analytics/deploy/peerdb && docker compose up -d
    cd /path/to/dw-analytics/deploy/analytics && docker compose up -d prefect-postgres prefect-server prefect-worker cli api
    ```

4. Run the provision script to configure Prefect:

    ```shell
    ./scripts/cli.sh prefect provision dev
    ```

### Uninstall

To delete all the data and Docker images, run:

```shell
./scripts/docker_clean.sh
```
