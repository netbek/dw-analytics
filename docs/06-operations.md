# Operations

> [!NOTE]
> Examples that start with the `cli` command indicate that the command would be executed inside of the Docker container. `cli` can also be accessed outside of the Docker container at `./scripts/cli.sh`. The arguments and options are the same.

## CLI

The CLI container has 3 CLIs:

- `dbt`: the entire dbt CLI
- `prefect`: the entire Prefect CLI
- `cli`: a supplementary CLI that provides convenience commands for managing projects and using dbt and Prefect

To explore:

1. Log into the Docker container:

    ```shell
    docker compose exec cli bash
    ```

2. View the list of available commands:

    ```shell
    dbt --help
    prefect --help
    cli --help
    ```

## Dashboards

The following Docker containers provide dashboards. The `open.sh` script only opens a URL or application, so remember to start the relevant container(s) beforehand.

| Service            | Command                            |
|--------------------|------------------------------------|
| `jupyter`          | `./scripts/open.sh jupyter`        |
| `prefect-server`   | `./scripts/open.sh prefect-server` |

## Database connections

The default settings for the Prefect Postgres server are:

```yaml
Host: localhost
Port: 29010
Username: prefect
Password: prefect
Database: prefect
```

Examples:

| Description                          | Command                                                           |
|--------------------------------------|-------------------------------------------------------------------|
| Use `psql` installed on host machine | `psql -h localhost -p 29010 -U prefect -d prefect`                |
| Use `psql` installed in container    | `docker compose exec prefect-postgres psql -U prefect -d prefect` |

## Monitoring

The resource usage of the Docker containers can be monitored with [https://github.com/netbek/dw-monitor](https://github.com/netbek/dw-monitor).

## Networking

Ports can optionally be exposed. The configuration is loaded from `./.env` during startup.

| Service            | Port  | Protocol              |
|--------------------|-------|-----------------------|
| `prefect-postgres` | 29010 | Postgres              |
| `prefect-server`   | 29020 | HTTP                  |
| `jupyter`          | 29030 | HTTP                  |

## Resources

- [ClickHouse docs](https://clickhouse.com/docs)
- [Jupyter Notebook docs](https://jupyter-notebook.readthedocs.io/en/latest/)
- [Postgres docs](https://www.postgresql.org/docs/current/index.html)
- [Prefect docs](https://docs.prefect.io)
