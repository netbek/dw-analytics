# dw-analytics

## Guides

- [Installation](docs/01-install.md)
- [Projects](docs/02-projects.md)
- [dbt](docs/03-dbt.md)
- [Jupyter Notebook](docs/04-jupyter.md)
- [API](docs/05-api.md)
- [PeerDB](docs/06-peerdb.md)
- [Prefect](docs/07-prefect.md)
- [Operations](docs/08-operations.md)

## Overview

```shell
.
├── .dbt
│   └── # dbt profiles
├── .devcontainer
│   └── # VS Code dev container configuration
├── .env_files
│   └── # .env files used in Docker containers
├── .prefect
│   └── # Prefect profiles
├── docker
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
├── .bashrc # Shell configuration used in CLI container
├── .env # Environment variables used by Docker Compose (not containers)
├── .gitconfig # Git configuration used in CLI container
├── .gitignore # Git configuration used in CLI container
├── .sqlfluff # SQL linter configuration used in CLI container
├── .sqlfluffignore # SQL linter configuration used in CLI container
├── .yamllint # YAML linter configuration used in CLI container
├── docker-compose.yml # Docker configurations for services, volumes and networking
├── pyproject.toml # Configuration of Python tools in CLI container
├── requirements_api.txt # Extra Python dependencies of API container
├── requirements_base.txt # Python dependencies of all containers
├── requirements_dev.txt # Python development dependencies
└── requirements_jupyter.txt # Extra Python dependencies of Jupyter container
```

## License

Copyright (c) 2023 Hein Bekker. Licensed under the GNU Affero General Public License, version 3.
