# Development Setup Guide

This document explains how to set up a local ClickHouse database for development and testing.

## Prerequisites

- Docker and Docker Compose
- Make (for convenient commands)

## Quick Start

1. **Start the ClickHouse database:**
   ```bash
   make db-up
   ```

2. **Verify the database is running:**
   ```bash
   make db-status
   ```

3. **Run tests:**
   ```bash
   make test
   ```

## Available Commands

### Database Management

- `make db-up` - Start ClickHouse in the background
- `make db-down` - Stop ClickHouse and remove containers
- `make db-logs` - View ClickHouse logs in real-time
- `make db-status` - Check if ClickHouse is running
- `make db-reset` - Stop database and delete all data (fresh start)
- `make db-cli` - Connect to ClickHouse CLI for manual queries

### Development Workflow

- `make deps` - Install Python dependencies
- `make test` - Run all tests
- `make test-pep249` - Run only PEP 249 compliance tests
- `make test-sqlalchemy` - Run only SQLAlchemy compatibility tests
- `make lint` - Format and lint code

## Database Configuration

The ClickHouse instance runs with the following default settings:

- **Host:** localhost
- **Native Port:** 9000 (for Python driver)
- **HTTP Port:** 8123 (for web interface)
- **User:** default
- **Password:** (empty - no authentication required)
- **Database:** default

These settings match your `.env` file and are automatically picked up by the tests.

## Accessing ClickHouse

### Via Python (your library)
```python
from asynch import Connection

async with Connection(dsn="clickhouse://default@localhost:9000/default") as conn:
    # Your code here
```

### Via Web Interface
Open http://localhost:8123/play in your browser for a web-based query interface.

### Via Command Line
```bash
make db-cli
```

## Example Usage

Here's a quick test to verify everything works:

```python
import asyncio
from asynch import Connection
from asynch.cursors import DictCursor

async def test():
    async with Connection(dsn="clickhouse://default@localhost:9000/default") as conn:
        async with conn.cursor(cursor=DictCursor) as cursor:
            await cursor.execute("SELECT version()")
            result = await cursor.fetchone()
            print("ClickHouse version:", result[0])

asyncio.run(test())
```

## Troubleshooting

### Database won't start
- Check if ports 9000 and 8123 are available: `lsof -i :9000,8123`
- View logs: `make db-logs`
- Reset everything: `make db-reset`

### Tests fail with connection errors
- Ensure database is running: `make db-status`
- Check your `.env` file matches the database configuration
- Verify the database is healthy: `make db-cli` and try `SELECT 1`

### Fresh start
If you encounter any issues, you can reset everything:
```bash
make db-down
make db-reset
make db-up
```

## Data Persistence

Database data is stored in a Docker volume named `asynch_clickhouse_data`. This means your data persists between container restarts, but is removed when you run `make db-reset`.

## Notes

- The setup uses no authentication for simplicity in development
- ClickHouse is configured to accept connections from any IP for development convenience
- Some SQLAlchemy tests may fail due to ClickHouse not supporting traditional transactions - this is expected behavior