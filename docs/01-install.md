# Installation

## Development

1. Install Docker and Docker Compose:

    ```shell
    ./scripts/install_docker.sh
    ```

2. Run the install script:

    ```shell
    ./scripts/install.sh
    ```

3. Start the ClickHouse container:

    ```shell
    docker compose up -d prefect-postgres prefect-server prefect-worker cli
    ```

    Wait for the container statuses to change to started and healthy.

4. Start the Prefect containers:

    ```shell
    docker compose up -d prefect-postgres prefect-server prefect-worker cli
    ```

    Wait for the container statuses to change to started and healthy.

5. Run the provision script to configure ClickHouse and Prefect:

    ```shell
    ./scripts/cli.sh prefect provision dev
    ```

## Optional extras

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
# Start ClickHouse
alias ach="cd /path/to/dw-clickhouse && docker compose up -d"

# Start Prefect
alias adw="cd /path/to/dw-analytics && docker compose up prefect-postgres prefect-server prefect-worker cli"

# Stop ClickHouse
alias sch="cd /path/to/dw-clickhouse && docker compose down"

# Stop Prefect
alias sdw="cd /path/to/dw-analytics && docker compose down"

# Start Prefect and Jupyter in detached mode, and open Jupyter
alias jdw="cd /path/to/dw-analytics && docker compose up -d prefect-postgres prefect-server prefect-worker cli jupyter && ./scripts/open.sh jupyter"

# Start Prefect in detached mode, and open VS Code
alias cdw="cd /path/to/dw-analytics && docker compose up -d prefect-postgres prefect-server prefect-worker cli && ./scripts/open.sh vscode"
```

Set `/path/to/` to the location of the repositories on your machine.

## Uninstall

To delete all the data and Docker images, run:

```shell
./scripts/uninstall.sh
```
