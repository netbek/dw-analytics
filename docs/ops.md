# Ops

> [!NOTE]
> Examples that start with the `cli` command indicate that the command would be executed inside of the Docker container. `cli` can also be accessed outside of the Docker container at `./scripts/cli.sh`. The arguments and options are the same.

## CLI

The Prefect CLI container has 3 CLIs:

- `dbt`: the entire dbt CLI
- `prefect`: the entire Prefect CLI
- `cli`: a supplementary CLI that provides convenience commands for managing projects and using dbt and Prefect

To explore:

1. Log into the Docker container:

    ```shell
    docker compose exec prefect-cli bash
    ```

2. View the list of available commands:

    ```shell
    dbt --help
    prefect --help
    cli --help
    ```

## GUI

The following Docker containers provide a GUI. The `open.sh` script only opens a URL or application, so remember to start the relevant container(s) beforehand.

| Service            | Description               | Command                            |
|--------------------|---------------------------|------------------------------------|
| `cadvisor`         | cAdvisor dashboard        | `./scripts/open.sh cadvisor`       |
| `grafana`          | Grafana dashboard         | `./scripts/open.sh grafana`        |
| `jupyter`          | Jupyter index view        | `./scripts/open.sh jupyter`        |
| `prefect-server`   | Prefect server dashboard  | `./scripts/open.sh prefect-server` |
| `prometheus`       | Prometheus dashboard      | `./scripts/open.sh prometheus`     |

TODO Add note about connection settings for ClickHouse and Prefect Postgres

## Monitoring

### Container metrics

Start monitoring the containers:

```shell
docker compose up cadvisor prometheus grafana
```

Open the Grafana dashboard:

```shell
./scripts/open.sh grafana
```

Stop monitoring the containers:

```shell
docker compose down cadvisor prometheus grafana
```

### ClickHouse

TODO

## Networking

The following ports are exposed:

| Service            | Port  | Protocol              |
|--------------------|-------|-----------------------|
| clickhouse         | 29000 | HTTP                  |
| clickhouse         | 29001 | Native/TCP            |
| clickhouse         | 29002 | Postgres emulation    |
| prefect-postgres   | 29010 | Postgres              |
| prefect-server     | 29020 | HTTP                  |
| jupyter            | 29030 | HTTP                  |
| cadvisor           | 29040 | HTTP                  |
| prometheus         | 29050 | HTTP                  |
| grafana            | 29060 | HTTP                  |

The configuration is loaded from `./.env` during startup. The default values are in `./template_env/docker-compose.env`.

## Resources

- [cAdvisor docs](https://github.com/google/cadvisor/blob/master/README.md)
- [ClickHouse docs](https://clickhouse.com/docs)
- [Grafana docs](https://grafana.com/docs/grafana/latest/)
- [Jupyter Notebook docs](https://jupyter-notebook.readthedocs.io/en/latest/)
- [Postgres docs](https://www.postgresql.org/docs/current/index.html)
- [Prefect docs](https://docs.prefect.io)
- [Prometheus docs](https://prometheus.io/docs/introduction/overview/)
