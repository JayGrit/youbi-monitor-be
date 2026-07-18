# monitor-be

`monitor-be` is the Spring Boot backend for the YouBi monitor. It reads the
shared MySQL database, exposes monitor APIs, proxies lifecycle actions to
`distributor`, and manages account/static-asset metadata used by production
services.

## Role

- Task monitor, task detail and route/progress APIs.
- Account overview, account profile, cooldown and platform status APIs.
- Operator diagnostic lookup and artifact access.
- Failure log, queue monitor and static asset APIs.
- AliDrive transfer helpers and MinIO-backed upload work directories.
- Distributor lifecycle proxy for stop, retry, restart and speaker reset.

It should not directly run media processing, browser automation or upload work.
Those responsibilities belong to the service workers.

## Run

```bash
cd /Users/hoshuuch/Money/YouBi/monitor/monitor-be
mvn spring-boot:run
```

Default port: `8200`.

## Configuration

Defaults are in `src/main/resources/application.yml`.

- `SPRING_DATASOURCE_URL`, `SPRING_DATASOURCE_USERNAME`,
  `SPRING_DATASOURCE_PASSWORD`: override MySQL.
- `YDBI_DISTRIBUTOR_BASE_URL`: default `http://127.0.0.1:8210`.
- `YDBI_OPERATOR_BASE_URL`: operator compatibility API base.
- `YDBI_MINIO_*`: MinIO endpoint, credentials and bucket.
- `YDBI_MONITOR_UPLOAD_WORK_DIR`: temporary upload work directory.
- `YDBI_ALIDRIVE_*`: AliDrive account and work directory settings.

## Docker

```bash
docker build -t monitor-be:local .
docker run --rm -p 8200:8200 monitor-be:local
```

## Checks

```bash
mvn -DskipTests package
```
