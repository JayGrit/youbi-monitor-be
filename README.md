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
- `GET /api/bilibili/account`
- `GET /api/bilibili/account?accountKey=default`
- `GET /api/bilibili/accounts`
- `POST /api/bilibili/account/qrcode`
- `POST /api/bilibili/account/qrcode?accountKey=default`
- `POST /api/bilibili/account/{accountKey}/qrcode/{authCode}/poll`
- `POST /api/bilibili/account/renew`
- `POST /api/bilibili/account/renew?accountKey=default`
- `POST /api/bilibili/upload`

The service only reads existing `yd_task` and stage tables. It does not create,
resume, or mutate processing tasks.
