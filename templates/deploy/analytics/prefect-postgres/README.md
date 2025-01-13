# prefect-postgres

## How to extend

1. Copy the configuration files from the image and amend:

    ```shell
    docker compose run --rm prefect-postgres bash -c "cat ./var/lib/postgresql/data/pg_hba.conf" > ./deploy/prefect-postgres/pg_hba.conf
    docker compose run --rm prefect-postgres bash -c "cat ./var/lib/postgresql/data/postgresql.conf" > ./deploy/prefect-postgres/postgresql.conf
    ```

    [Read the official docs for details](https://github.com/docker-library/docs/blob/master/postgres/README.md#database-configuration).

    Example of Docker Compose file:

    ```yaml
    services:
      prefect-postgres:
        command: postgres -c config_file=/etc/postgresql/postgresql.conf
        volumes:
          - ./deploy/prefect-postgres/pg_hba.conf:/etc/postgresql/pg_hba.conf:ro
          - ./deploy/prefect-postgres/postgresql.conf:/etc/postgresql/postgresql.conf:ro
    ```

2. Add initialization scripts to `./prefect-postgres/docker-entrypoint-initdb.d`

    [Read the official docs for details](https://github.com/docker-library/docs/blob/master/postgres/README.md#initialization-scripts).

    Example of Docker Compose file:

    ```yaml
    services:
      prefect-postgres:
        volumes:
          - ./deploy/prefect-postgres/docker-entrypoint-initdb.d:/docker-entrypoint-initdb.d:ro
    ```
