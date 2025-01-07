# Documentation

## Guides

- [Installation](01-install.md)
- [Projects](02-projects.md)
- [dbt](03-dbt.md)
- [Jupyter Notebook](04-jupyter.md)
- [API](05-api.md)
- [PeerDB](06-peerdb.md)
- [Prefect](07-prefect.md)
- [Operations](08-operations.md)

## Architecture overview

```mermaid
graph LR
    subgraph app
        A[app]
    end
    subgraph postgres
        B[postgres<br>port: 5432]
    end
    subgraph dw
        subgraph clickhouse
            C[clickhouse<br>HTTP port: 29200<br>TCP port: 29201]
        end
        subgraph peerdb
            D1[catalog<br>port: 9901]
            D2[temporal<br>port: 7233]
            D3[temporal-admin-tools]
            D4[flow-api<br>port: 8112-8113]
            D5[flow-snapshot-worker]
            D6[flow-worker]
            D7[minio<br>port: 9001-9002]
            D8[peerdb<br>port: 9900]
            D9[peerdb-ui<br>port: 3000]
            D10[temporal-ui<br>port: 8085]
            D9 --> D8
        end
        subgraph analytics
            E1[prefect-postgres<br>port: 29110]
            E2[prefect-server<br>port: 29120]
            E3[prefect-worker]
            E4[jupyter<br>port: 29130]
            E5[api<br>port: 29140]
            E6[cli<br>port: 29150]
            E2 --> E3
            E2 --> E1
            E6 --> E2
        end
        subgraph monitor
            F1[cadvisor<br>port: 29300]
            F2[prometheus<br>port: 29310]
            F3[grafana<br>port: 29320]
            F3 --> F2
            F2 --> F1
        end
    end

    A --> E5
    A --> B
    A --> C
    D8 --> B
    D8 --> C
    F1 --> C
    F1 --> E1
    F1 --> E2
    F1 --> E6
    E3 --> C
    E4 --> C
    E5 --> C
    E6 --> C
```

## Resources

- [ClickHouse docs](https://clickhouse.com/docs)
- [ClickHouse SQL reference](https://clickhouse.com/docs/en/sql-reference)
- [dbt reference](https://docs.getdbt.com/reference/references-overview)
- [Jinja syntax reference](https://jinja.palletsprojects.com/en/3.1.x/templates/)
- [Jupyter Notebook docs](https://jupyter-notebook.readthedocs.io/en/latest/)
- [PeerDB docs](https://docs.peerdb.io)
- [Postgres docs](https://www.postgresql.org/docs/current/index.html)
- [Postgres SQL reference](https://www.postgresql.org/docs/current/sql-commands.html)
- [Prefect docs](https://docs.prefect.io)
- [pytest docs](https://docs.pytest.org/en/8.3.x/)
