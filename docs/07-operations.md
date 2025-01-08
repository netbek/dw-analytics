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

## Monitoring

| URL                                                                    | Description                                                       |
|------------------------------------------------------------------------|-------------------------------------------------------------------|
| [http://localhost:29000](localhost:29000)                              | Tilt: Status of Docker containers (development only)              |
| [http://localhost:29320](http://localhost:29320)                       | Grafana: Resource usage of Docker containers                      |
| [http://localhost:29200/dashboard](http://localhost:29200/dashboard)   | ClickHouse dashboard                                              |
| [http://localhost:3000/mirrors](http://localhost:3000/mirrors)         | PeerDB mirrors                                                    |
| [http://localhost:29120](http://localhost:29120)                       | Prefect dashboard                                                 |

## Database connections

The default settings for the Prefect Postgres server are:

```yaml
Host: localhost
Port: 29110
Username: prefect
Password: prefect
Database: prefect
```

Examples:

| Description                          | Command                                                           |
|--------------------------------------|-------------------------------------------------------------------|
| Use `psql` installed on host machine | `psql -h localhost -p 29110 -U prefect -d prefect`                |
| Use `psql` installed in container    | `docker compose exec prefect-postgres psql -U prefect -d prefect` |

## Resources

- [ClickHouse docs](https://clickhouse.com/docs)
- [Jupyter Notebook docs](https://jupyter-notebook.readthedocs.io/en/latest/)
- [PeerDB docs](https://docs.peerdb.io)
- [Postgres docs](https://www.postgresql.org/docs/current/index.html)
- [Prefect docs](https://docs.prefect.io)
