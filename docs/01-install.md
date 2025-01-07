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

3. Clone PeerDB:

    ```shell
    ./scripts/install.sh peerdb
    ```

4. Create the .env files for deployment:

    ```shell
    ./scripts/env.sh dev
    ```

    When prompted, enter the suggested values:

    ```shell
    clickhouse_default_username: default
    clickhouse_default_password: default
    clickhouse_default_database: default
    prefect_postgres_default_username: postgres
    prefect_postgres_default_password: postgres
    prefect_postgres_default_database: postgres
    prefect_postgres_prefect_username: prefect
    prefect_postgres_prefect_password: prefect
    prefect_postgres_prefect_database: prefect
    ```

5. Start the services:

    ```shell
    tilt up --port 29000
    ```

    Wait a few minutes for the Docker images to be built. To check the progress, open [http://localhost:29000](http://localhost:29000).

6. Run the provision script to configure Prefect:

    ```shell
    ./scripts/cli.sh prefect provision dev
    ```

7. For each project, clone its repo:

    ```shell
    cd analytics/projects
    git clone ... PROJECT_NAME
    ```

8. For each project, install its dependencies:

    ```shell
    ./scripts/cli.sh project refresh PROJECT_NAME
    ```

9. For each project, set up PeerDB and sync the ClickHouse database:

    ```shell
    ./scripts/cli.sh peerdb install PROJECT_NAME
    ```

    To check the progress, open [http://localhost:3000/mirrors](http://localhost:3000/mirrors).

### Optional extras

#### VS Code

Download [VS Code](https://code.visualstudio.com/). After installing VS Code, install the [Dev Containers extension](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-containers):

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
# Start warehouse
alias adw="cd /path/to/dw-analytics && tilt up --port 29000"

# Stop warehouse
alias sdw="cd /path/to/dw-analytics && tilt down"

# Connect to ClickHouse
alias cch="cd /path/to/dw-analytics/deploy/clickhouse && docker compose exec clickhouse clickhouse-client --user analyst --password analyst"

# Open Jupyter
alias jdw="cd /path/to/dw-analytics/analytics/scripts/open.sh jupyter"

# Open VS Code
alias cdw="cd /path/to/dw-analytics/analytics/scripts/open.sh vscode"
```

Set `/path/to/` to the location of the repository on your machine.

### Uninstall

1. Start the services, if not already running:

    ```shell
    tilt up --port 29000
    ```

2. For each project, stop PeerDB syncing:

    ```shell
    ./scripts/cli.sh peerdb uninstall PROJECT_NAME
    ```

3. Delete all the data and Docker images:

    ```shell
    ./scripts/destroy.sh
    ```

## Production

### Install

1. [Docker Engine v23 or higher](https://docs.docker.com/engine/install/) and [Docker Compose v2 or higher](https://docs.docker.com/compose/install/). Follow the links for instructions or run this script:

    ```shell
    ./scripts/install.sh docker
    ```

2. Clone PeerDB:

    ```shell
    ./scripts/install.sh peerdb
    ```

3. Create the .env files for deployment:

    ```shell
    ./scripts/env.sh prod
    ```

4. Build the Docker images:

    ```shell
    ./scripts/build.sh
    ```

5. Start the services:

    ```shell
    cd /path/to/dw-analytics/deploy/clickhouse && docker compose up -d
    cd /path/to/dw-analytics/deploy/peerdb && docker compose up -d
    cd /path/to/dw-analytics/deploy/analytics && docker compose up -d prefect-postgres prefect-server prefect-worker cli api
    ```

6. Run the provision script to configure Prefect:

    ```shell
    ./scripts/cli.sh prefect provision dev
    ```

7. For each project, clone its repo:

    ```shell
    cd analytics/projects
    git clone ... PROJECT_NAME
    ```

8. For each project, install its dependencies:

    ```shell
    ./scripts/cli.sh project refresh PROJECT_NAME
    ```

9. For each project, set up PeerDB and sync the ClickHouse database:

    ```shell
    ./scripts/cli.sh peerdb install PROJECT_NAME
    ```

    To check the progress, open [http://localhost:3000/mirrors](http://localhost:3000/mirrors).

### Uninstall

1. Start the services, if not already running:

    ```shell
    cd /path/to/dw-analytics/deploy/clickhouse && docker compose up -d
    cd /path/to/dw-analytics/deploy/peerdb && docker compose up -d
    cd /path/to/dw-analytics/deploy/analytics && docker compose up -d prefect-postgres prefect-server prefect-worker cli api
    ```

2. For each project, stop PeerDB syncing:

    ```shell
    ./scripts/cli.sh peerdb uninstall PROJECT_NAME
    ```

3. Delete all the data and Docker images:

    ```shell
    ./scripts/destroy.sh
    ```
