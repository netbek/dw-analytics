# dw-analytics

## Guides

- [Installation](docs/install.md)
- [Projects](docs/projects.md)
- [dbt](docs/dbt.md)
- [Jupyter Notebook](docs/jupyter.md)
- [Prefect](docs/prefect.md)
- [Ops](docs/ops.md)

## Overview

The stack consists of these Docker containers:

| Service           | Description                                                    |
| ----------------- | -------------------------------------------------------------- |
| `clickhouse`      | ClickHouse server that manages the reporting database          |
| `prefect-postgres`| Postgres server that manages the Prefect database              |
| `prefect-server`  | Prefect backend and API                                        |
| `prefect-worker`  | Process that executes Python workflows                         |
| `prefect-cli`     | Development environment for dbt, Prefect, and VS Code          |
| `jupyter`         | Jupyter Notebook server                                        |
| `cadvisor`        | cAdvisor service that collects and processes resource usage    |
| `prometheus`      | Prometheus service that records metrics of Docker containers   |
| `grafana`         | Grafana service that visualizes metrics of Docker containers   |

The repository is structured as follows:

```shell
.
├── .dbt
│   └── # dbt profiles
├── .devcontainer
│   └── # VS Code dev container configuration
├── .env_files
│   └── # .env files used in Docker containers
├── .ipython
│   └── # Jupyter notebook configuration
├── .jupyter
│   └── # Jupyter server configuration
├── .prefect
│   └── # Prefect profiles
├── infrastructure
│   └── # Docker container configurations
├── package
│   └── # Supplementary CLI and common utilities
├── projects
│   └── # Data models, Python tasks, workflows, notebooks and tests
├── scripts
│   └── # Installation and convenience scripts
├── template_env
│   └── # Template for new .env files
├── template_project
│   └── # Template for new projects
├── .bashrc # Shell configuration used in prefect-cli
├── .env # Environment variables used by Docker Compose (not containers)
├── .gitconfig # Git configuration used in prefect-cli
├── .gitignore # Git configuration used in prefect-cli
├── .sqlfluff # SQL linter configuration used in prefect-cli
├── .sqlfluffignore # SQL linter configuration used in prefect-cli
├── .yamllint # YAML linter configuration used in prefect-cli
├── docker-compose.yml # Docker configurations for services, volumes and networking
├── install.yml # Installation script configuration
├── pyproject.toml # Configuration of Python tools in prefect-cli
├── requirements.txt # Python dependencies in Prefect and Jupyter containers
└── requirements_dev.txt # Python dev dependencies in Prefect and Jupyter containers
```

## License

Copyright (c) 2023 Hein Bekker. Licensed under the GNU Affero General Public License, version 3.
