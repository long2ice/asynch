# Project notes

## Table of contents

- [How to test](#how-to-test)

### How to test

A way (but not the sole one) to do it:

1. Make sure the virtual environment (venv) is activated (use `poetry shell` to activate it if necessary);
2. Create a `.env` file if necessary (you can copypaste the contents of the [example.env](./example.env) file);
3. Inject the environment variable from the `.env` file into the virtual environment -> on Ubuntu you can do it with `export $(cat .env | xargs)`;
4. Run a "clickhouse-server" container in a terminal. For example, `docker run --rm --name clickhouse-server -p 8123:8123 -p 9000:9000 --ulimit nofile=262144:262144 -v clickhouse_data:/var/lib/clickhouse -v clickhouse_logs:/var/log/clickhouse-server -e CLICKHOUSE_USER=ch -e CLICKHOUSE_PASSWORD=asynch -e CLICKHOUSE_DB=test clickhouse/clickhouse-server`;
5. Launch `make test` in another terminal.
