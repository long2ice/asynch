# Project notes

## Table of contents

- [How to test](#how-to-test)

### How to test

A way (but not the sole one) to do it:

1. make sure the virtual environment is activated (use `poetry shell` to activate it if necessary);
2. create a `.env` file (you can copypaste the contents of the [example.env](./example.env) file);
3. inject the environment variable, on Ubuntu you can do it like `export $(cat .env | xargs)`;
4. run `docker run --name clickhouse-server -p 8123:8123 -p 9000:9000 --ulimit nofile=262144:262144 -v clickhouse_data:/var/lib/clickhouse -v clickhouse_logs:/var/log/clickhouse-server -e CLICKHOUSE_USER=ch -e CLICKHOUSE_PASSWORD=P@s5W0rD -e CLICKHOUSE_DB=test clickhouse/clickhouse-server` in a terminal (assuming the contents of the [example.env](./example.env) file are copypasted, otherwise adjust the environment variables accordingly);
5. run `make test` in another terminal.
