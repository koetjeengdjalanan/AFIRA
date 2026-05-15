# AFIRA

AFIRA, Aruba Fortigate Information Rate Automation, collects HPE Aruba Central
network metrics and writes them to InfluxDB for dashboarding in Grafana.

The application is intended to run continuously. Each cycle authenticates to HPE
Aruba Central, fetches site, device, WLAN, client, and hardware metrics, writes
the resulting points to InfluxDB, then sleeps before starting the next cycle.
Runtime failures are logged and retried on the next loop so the process is safe
to keep running in a container.

## What AFIRA Collects

- Site health and inventory data.
- Network device inventory and status.
- WLAN configuration and throughput trends.
- Site details such as device locations, Wi-Fi client locations, clients, and
  web application metrics.
- Access point and switch hardware details such as CPU, memory, power, and
  switch hardware data.

## Requirements

- Python 3.12 or newer.
- `uv` for local development commands, or `pip` with `requirements.txt`.
- Docker and Docker Compose if you want the bundled InfluxDB and Grafana stack.
- HPE Aruba Central OAuth client credentials.
- An InfluxDB 2.x bucket, organization, and token.

## Initial Setup

Create the local environment file and runtime folders:

```sh
sh init.sh
```

Then edit `.env` and set the values for your environment. The most important
settings are:

```env
AFIRA_LOOP_SLEEP_SECONDS=300
INFLUXDB_URL=http://localhost:8086
DOCKER_INFLUXDB_INIT_ORG=OneTeam
DOCKER_INFLUXDB_INIT_BUCKET=afira
DOCKER_INFLUXDB_INIT_ADMIN_TOKEN=my-super-secret-auth-token
LOG_FILE_PATH=./logs/afira.log
LOG_INFLUXDB_ENABLED=True
LOG_INFLUXDB_LEVEL=INFO
LOG_INFLUXDB_MEASUREMENT=afira_logs
```

Create `creds.yaml` in the project root with your HPE Aruba Central OAuth
settings:

```yaml
new_central:
  token_url: "https://example.com/oauth2/token"
  base_url: "https://example.com"
  client_id: "00000000-0000-0000-0000-000000000000"
  client_secret: "replace-with-your-client-secret"
```

`creds.yaml` and `.env` are intentionally excluded from Docker builds so secrets
do not get baked into images.

## Run InfluxDB And Grafana

Start the supporting services:

```sh
docker compose up -d influxdb grafana renderer
```

Useful service URLs:

- InfluxDB: `http://localhost:8086`
- Grafana: `http://localhost:3000`

Stop the services when needed:

```sh
docker compose down
```

## Run AFIRA Locally

Install dependencies:

```sh
uv sync
```

Run the collector:

```sh
uv run python main.py
```

AFIRA will keep running until interrupted. Press `Ctrl+C` to request a graceful
shutdown.

If you prefer plain `pip`:

```sh
python3.12 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
python main.py
```

## Run AFIRA In Docker

Build the image from the repository root:

```sh
docker build -f .docker/afira.dockerfile -t afira .
```

If InfluxDB is running from this repository's Compose stack, set the AFIRA
container to use the Compose service name:

```sh
docker run --rm \
  --name afira \
  --env-file .env \
  --network afira_default \
  -e INFLUXDB_URL=http://influxdb:8086 \
  -v "$PWD/creds.yaml:/app/creds.yaml:ro" \
  -v "$PWD/logs:/app/logs" \
  afira
```

Depending on your Compose project name, the network may be named differently.
Check it with:

```sh
docker network ls
```

Stop the container gracefully:

```sh
docker stop afira
```

AFIRA handles `SIGTERM` and `SIGINT`, so `docker stop` wakes the sleep loop and
lets the process exit cleanly.

## Runtime Configuration

`AFIRA_LOOP_SLEEP_SECONDS` controls the delay between collection cycles.

```env
AFIRA_LOOP_SLEEP_SECONDS=300
```

The default is 300 seconds, or 5 minutes. Use a higher value to reduce HPE API
and InfluxDB write volume. Use a lower value if you need fresher dashboard data.

`LOG_LEVEL` controls file and console logging. `LOG_INFLUXDB_LEVEL` controls
the minimum level written to InfluxDB. Log records are written to the configured
InfluxDB bucket using `LOG_INFLUXDB_MEASUREMENT`, which defaults to
`afira_logs`. Set `LOG_INFLUXDB_ENABLED=False` only when you want to disable
InfluxDB log storage.

## Development Checks

Run linting:

```sh
uv run --with ruff ruff check .
```

Run formatting checks:

```sh
uv run --with black black --check .
```

Run tests:

```sh
uv run --with pytest --with pytest-cov --with pytest-mock pytest
```

The current pytest configuration may warn that `libs` was not imported because
the source directory is named `lib`. The warning does not prevent the tests from
passing.

## Troubleshooting

- If AFIRA starts but logs InfluxDB connection errors, confirm `INFLUXDB_URL`,
  token, org, and bucket match your running InfluxDB instance.
- If the container cannot find HPE credentials, confirm `creds.yaml` is mounted
  at `/app/creds.yaml`.
- If logs are not written, confirm `LOG_FILE_PATH` points to a writable `.log`
  file path.
- If Docker cannot reach InfluxDB, make sure the AFIRA container is attached to
  the same Docker network as the InfluxDB container.
