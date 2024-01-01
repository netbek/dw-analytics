# dbt

## CLI

The Prefect CLI container has 2 CLIs for interacting with dbt:

- `dbt`: the entire dbt CLI
- `cli dbt`: a supplementary CLI that provides convenience commands for using dbt

To explore:

1. Log into the Docker container:

    ```shell
    docker compose exec prefect-cli bash
    ```

2. View the list of available commands:

    ```shell
    dbt --help
    cli dbt --help
    ```

## Resources

- [ClickHouse SQL reference](https://clickhouse.com/docs/en/sql-reference)
- [dbt reference](https://docs.getdbt.com/reference/references-overview)
- [Jinja syntax reference](https://jinja.palletsprojects.com/en/3.1.x/templates/)
- [Postgres SQL reference](https://www.postgresql.org/docs/current/sql-commands.html)
