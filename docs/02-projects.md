# Projects

> [!NOTE]
> Examples that start with the `cli` command indicate that the command would be executed inside of the Docker container. `cli` can also be accessed outside of the Docker container at `./scripts/cli.sh`. The arguments and options are the same.

## Overview

Projects are structured as follows. To explore, open `./projects/tutorial`.

```shell
.
└── projects
    └── <PROJECT_NAME>
        ├── api
        │   └── # API
        ├── config
        │   └── settings.py # Settings specific to this project
        ├── dbt
        │   └── # dbt configuration, models, and tests
        ├── flows
        │   └── # Python tasks and workflows
        ├── notebooks
        │   └── # Jupyter notebooks
        ├── tests
        │   └── # Tests of Python code
        ├── peerdb.yaml # PeerDB configuration
        ├── prefect.yaml # Prefect configuration
        └── README.md # Project documentation
```

When more than 1 flow or notebook needs a certain function, consider adding a utilities file or directory. The following example shows 2 approaches:

```shell
.
└── projects
    └── <PROJECT_NAME>
        ├── flows
        │   └── harvest
        │       ├── harvest.py
        │       └── utils.py # Specific to this flow
        ├── notebooks
        │   └── forecast.ipynb
        └── utils
            └── math.py # Common utilities available to all flows and notebooks in this project
```

## Create a project

To create a project, run:

```shell
cli project init PROJECT_NAME
```

This command copies the directories and files from `./templates/project` to `./projects/<PROJECT_NAME>`.

## Linting and testing a project

To lint the SQL and YAML files of a project, run:

```shell
cli project lint PROJECT_NAME
```

To run all of the dbt and Python tests of a project, run:

```shell
cli project test PROJECT_NAME
```

To run specific Python tests, use the `pytest` command. Here's an example filesystem structure:

```shell
.
└── projects
    └── farm
        ├── flows
        │   └── harvest
        │       ├── harvest.py
        │       └── utils.py
        ├── tests
        │   ├── flows
        │   │   └── harvest
        │   │       ├── test_harvest.py
        │   │       └── test_utils.py
        │   └── utils
        │       └── test_math.py
        └── utils
            └── math.py
```

To run only the tests in the `utils` directory, not the `flows` directory, run:

```shell
cd projects/farm
pytest tests/utils
```

## Housekeeping

To delete dbt artifacts, install dbt dependencies, and sync the global dbt macros (`./projects/<PROJECT_NAME>/dbt/macros/dbt`), run:

```shell
cli project refresh PROJECT_NAME
```

## Delete a project

To delete a project, run:

```shell
cli project delete PROJECT_NAME
```

> [!CAUTION]
> The project directory `./projects/<PROJECT_NAME>` will be deleted from disk, and the flows and deployments will be deleted from the database.

## Resources

- [pytest docs](https://docs.pytest.org/en/8.3.x/)
