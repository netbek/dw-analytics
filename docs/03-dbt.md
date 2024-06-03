# dbt

## CLI

The CLI container has 2 CLIs for interacting with dbt:

- `dbt`: the entire dbt CLI
- `cli dbt`: a supplementary CLI that provides convenience commands for using dbt

To explore:

1. Log into the Docker container:

    ```shell
    docker compose exec cli bash
    ```

2. View the list of available commands:

    ```shell
    dbt --help
    cli dbt --help
    ```

## Documentation

The dbt documentation is stored in `./projects/<PROJECT_NAME>/dbt/docs`.

To view the docs:

1. Start the LiveReload server:

    ```shell
    cli dbt docs serve
    ```

2. Go to [http://localhost:29050](http://localhost:29050) or run the `open` script:

    ```shell
    ./scripts/open.sh dbt-docs
    ```

After saving changes to the project configuration, macro files, or model files, the docs will be regenerated and the browser will reload the page.

To only generate the docs, run:

```shell
cli dbt docs generate
```

## Resources

- [ClickHouse SQL reference](https://clickhouse.com/docs/en/sql-reference)
- [dbt reference](https://docs.getdbt.com/reference/references-overview)
- [Jinja syntax reference](https://jinja.palletsprojects.com/en/3.1.x/templates/)
- [Postgres SQL reference](https://www.postgresql.org/docs/current/sql-commands.html)
