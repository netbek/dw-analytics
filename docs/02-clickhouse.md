# ClickHouse

## Database connections

The default settings for the ClickHouse server are:

```yaml
Host: localhost
Port: 29200
Username: analyst
Password: analyst
Database: analytics
```

Examples:

| Description                                       | Command                                                                                       |
|---------------------------------------------------|-----------------------------------------------------------------------------------------------|
| Use `clickhouse-client` installed on host machine | `clickhouse-client -h localhost -p 29201 -u analyst --password analyst -d analytics`          |
| Use `clickhouse-client` installed in container    | `docker compose exec clickhouse clickhouse-client -u analyst --password analyst -d analytics` |
| Use `psql` installed on host machine              | `psql -h localhost -p 29202 -U analyst -d analytics`                                          |

## Networking

| Service              | Port  | Protocol              |
|----------------------|-------|-----------------------|
| `clickhouse`         | 29200 | HTTP                  |
| `clickhouse`         | 29201 | Native/TCP            |
| `clickhouse`         | 29202 | Postgres emulation    |

## Resources

- [ClickHouse docs](https://clickhouse.com/docs)
- [ClickHouse SQL reference](https://clickhouse.com/docs/en/sql-reference)
