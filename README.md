# monitor-be

Spring Boot 3 / Java 17 backend for the YouBi video generation monitor.

## Upload execution

monitor-be only exposes monitoring and management APIs. Upload execution is handled outside this service.

Database-backed account overview and configuration APIs remain active.

## Run

```bash
mvn spring-boot:run
```

Default port: `8200`.

## Docker

```bash
docker build -t monitor-be:local .
docker run --rm -p 8200:8200 monitor-be:local
```

Override Spring datasource settings with environment variables such as
`SPRING_DATASOURCE_URL`, `SPRING_DATASOURCE_USERNAME`, and
`SPRING_DATASOURCE_PASSWORD`.

## API

- `GET /api/video-tasks/monitor?limit=100`
- `GET /api/accounts/overview`
- `GET /api/accounts/{platform}/{accountKey}`

The service only reads existing `task` and stage tables. It does not create,
resume, or mutate processing tasks.
