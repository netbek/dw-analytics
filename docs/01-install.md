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

3. Start the ClickHouse and Prefect containers:

    ```shell
    docker compose up -d clickhouse prefect-postgres prefect-server prefect-worker cli
    ```

    Wait for the container statuses to change to started and healthy.

4. Run the provision script to configure ClickHouse and Prefect:

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

GUI client for Postgres, ClickHouse and more. To install, run:

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
# Start ClickHouse and Prefect
alias adw="cd /path/to/dw-analytics && docker compose up clickhouse prefect-postgres prefect-server prefect-worker cli"

# Stop all services
alias sdw="cd /path/to/dw-analytics && docker compose down"

# Start ClickHouse, Prefect and Jupyter in detached mode, and open Jupyter
alias jdw="cd /path/to/dw-analytics && docker compose up -d clickhouse prefect-postgres prefect-server prefect-worker cli jupyter && ./scripts/open.sh jupyter"

# Start ClickHouse and Prefect in detached mode, and open VS Code
alias cdw="cd /path/to/dw-analytics && docker compose up -d clickhouse prefect-postgres prefect-server prefect-worker cli && ./scripts/open.sh vscode"
```

Set `/path/to/dw-analytics` to the location of the repository on your machine.
