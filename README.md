# monitor-be

Spring Boot 3 / Java 17 backend for the YouBi video generation monitor.

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

- `GET /health`
- `GET /api/video-tasks/monitor?limit=100`
- `GET /api/biliup/status`
- `POST /api/biliup/login`
- `POST /api/biliup/renew`
- `GET /api/biliup/jobs/{id}`
- `POST /api/biliup/jobs/{id}/input`
- `POST /api/biliup/jobs/{id}/cancel`
- `GET /api/biliup/videos?type=all|pubing|pubed|notPubed`

The service only reads existing `yd_task` and stage tables. It does not create,
resume, or mutate processing tasks.
