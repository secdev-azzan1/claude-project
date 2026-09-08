# Running with Docker

## Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) installed and running (includes Docker Compose).

## Start the app

From the repository root:

```
docker compose up -d
```

The first run builds the backend and frontend images, which can take a few minutes. Every run after that is fast.

Check that everything is healthy:

```
docker compose ps
```

You should see `mongo`, `backend`, and `frontend` all listed as `healthy`.

## Open it

| What | URL |
|---|---|
| App (frontend) | http://localhost:3001 |
| Backend API docs (Swagger) | http://localhost:8011/docs |

## First run

The app starts empty — no connections, flows, or sample data are created for you. Add your own platform connections (NiFi, Kafka, etc.) from the **Connections** page once the app is open.

## Stop the app

```
docker compose down
```

Your data stays in place (stored in a Docker volume) — running `docker compose up -d` again picks up right where you left off. To wipe it and start fully fresh:

```
docker compose down -v
```
