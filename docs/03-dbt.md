# dbt

## Overview

dbt projects are structured as follows:

```shell
.
└── projects
    └── <PROJECT_NAME>
        └── dbt
            ├── analyses # https://docs.getdbt.com/docs/build/analyses
            ├── docs # Documentation
            ├── macros # https://docs.getdbt.com/docs/build/jinja-macros#macros
            ├── models # https://docs.getdbt.com/docs/build/models
            ├── seeds # https://docs.getdbt.com/docs/build/seeds
            ├── snapshots # https://docs.getdbt.com/docs/build/snapshots
            ├── tests # https://docs.getdbt.com/docs/build/data-tests
            ├── dbt_project.yml # https://docs.getdbt.com/reference/dbt_project.yml
            ├── dependencies.yml # https://docs.getdbt.com/docs/build/packages
            └── selectors.yml # https://docs.getdbt.com/reference/node-selection/yaml-selectors
```

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

To generate only the docs, run:

```shell
cli dbt docs generate
```

## Resources

- [ClickHouse SQL reference](https://clickhouse.com/docs/en/sql-reference)
- [dbt reference](https://docs.getdbt.com/reference/references-overview)
- [Jinja syntax reference](https://jinja.palletsprojects.com/en/3.1.x/templates/)
- [Postgres SQL reference](https://www.postgresql.org/docs/current/sql-commands.html)
