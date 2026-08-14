# ChangeLog

## 0.4

### 0.4.0

Major internal refactor: the protocol hot path is now compiled with Cython,
modeled on the sister project [asyncmy](https://github.com/long2ice/asyncmy).
The public API (`Connection`, `Cursor`, `DictCursor`, `Pool`, DSNs) is
unchanged.

#### Performance (200k-row SELECT benchmarks vs 0.3.x)

- `String` columns: ~9x faster (bulk sync parsing of length-prefixed values)
- `Array` columns: ~4x faster (batched offset reads, deque-based BFS)
- Mixed workloads: ~5x faster; most column types now match or beat
  `clickhouse-driver` (see the Performance section in the README)
- New benchmark suite under `benchmark/` (SELECT per column type, batched
  INSERT, concurrency via pool, pool overhead): `make benchmark`

#### Packaging & toolchain

- Python 3.11+ required (3.9/3.10 support dropped)
- Wheels are now platform-specific binary wheels built by cibuildwheel
  (Linux x86_64/arm64, Windows, macOS Intel/ARM); platforms without a wheel
  compile from sdist and need a C toolchain plus Cython
- Dependency management moved from Poetry to uv (PEP 735 dependency groups);
  contributors run `uv sync --all-groups --all-extras` (or `make deps`)
- Type information ships as generated `.pyi` stubs validated by stubtest
- CI matrix: Python 3.11–3.14, ClickHouse latest + LTS lines; PyPI
  publishing via trusted publishing (OIDC)

#### API

- `Connection.last_query` exposes the statistics the server reported for the
  most recent query (elapsed, rows/bytes progress, profile info); previously
  reachable only as `conn._connection.last_query` (#85)
- `Pool(idle_timeout=...)` reaps connections that have been idle for longer
  than the timeout, down to `minsize`. Without it the pool grows to its
  high-water mark and keeps every connection for the process lifetime, which
  is still the default (#137)
- Pool checkouts cost one liveness ping instead of two: a connection verified
  on release is not re-pinged when acquired again within `liveness_grace`
  seconds (1.0 by default)
- `lz4` and `zstd` moved into the `compression` extra. They were mandatory
  even though compression cannot work without the optional
  `clickhouse-cityhash`, so every install carried two unusable codec
  libraries. On Python 3.14+ the stdlib `compression.zstd` is used and no
  third-party zstd package is needed at all (#143)
- PEP 249 module surface: `apilevel`, `threadsafety`, `paramstyle`,
  `connect()` and the exception hierarchy are now importable from `asynch`;
  `Cursor.arraysize` is a read/write attribute. Additive only - no existing
  behaviour changes. Based on #159 by @turquoisehealth

#### Dependencies

- Removed `leb128` (hand-rolled unsigned LEB128; also fixes non-canonical
  varint encodings the signed encoder produced) and `pytz` (stdlib zoneinfo;
  `tzdata` is pulled in on Windows only)

#### Fixes

- Compressed inserts corrupted the stream and the server dropped the
  connection: the compressed writer's flush kept its buffer, re-compressing
  and re-sending every earlier byte. Reported in #149, fix based on #153 by
  @nils-borrmann-tacto
- A cancelled query left the connection permanently unusable: every later
  query raised "some records have not been fetched". `asyncio.CancelledError`
  is a `BaseException`, so the teardown branch was skipped and
  `is_query_executing` stayed set - which any `asyncio.timeout` around a
  query, or a web framework cancelling a request task, would trigger (#93)
- `connect_timeout` and `sync_request_timeout` were accepted but never used:
  a black-holed host hung on the OS connect timeout (taking `alt_hosts`
  failover with it), and a `ping()` against an accepted-but-silent server
  could stall a pool checkout indefinitely (#114)
- `send_receive_timeout` is now enforced on socket reads and writes; a server
  that accepts a connection and then goes silent no longer hangs the client
  indefinitely (#114)
- A missing codec package reported `UnknownCompressionMethod: Unknown
  compression method: 'lz4'`, which reads as a typo in a valid method name;
  it now says to install `asynch[compression]`
- Streaming forced `max_block_size`, so a `readonly=1` user could not stream
  at all, and the default buffer of 0 was rejected by the server outright.
  The setting is only sent when a buffer size was requested (#67)
- A secure connection without an explicit port used 9000 instead of 9440:
  the port default was applied before the scheme was known, so
  `clickhouses://host/db` and `Connection(secure=True)` both went to the
  plaintext port. Based on #162 by @stankudrow
- A server-side error (a bad query) disconnected the connection, so every SQL
  error cost a pooled connection even though the stream is left at a packet
  boundary and stays usable. Based on #150 by @nils-borrmann-tacto
- Inserting `None` into a non-Nullable column raised whatever the serializer
  happened to fail on (`TypeError: cannot convert 'NoneType' object to
  bytes`, `AttributeError: 'NoneType' object has no attribute 'year'`);
  it now reports the column and expected type. `input_format_null_as_default`
  keeps working. Based on #146 by @vizor-games
- `alt_hosts` never worked as a fallback: the first host's failure aborted
  the whole connect. Unreachable hosts are now skipped (with the socket torn
  down between attempts) and `NetworkError` is raised only if all fail;
  errors from a server that answers, such as bad credentials, propagate
  unchanged. Based on #136 by @vlad-zverev
- Pool connections that died while idle were handed back to callers:
  `_refresh` "reconnected" them, but `connect()` returns early on an
  apparently-open connection, so the reconnect was a no-op. Dead connections
  are now discarded and replaced. Based on #145 by @nils-borrmann-tacto
- `ping()` no longer lets `OperationalError` escape when the peer closes the
  connection mid-read, which broke transparent pool reconnects. Based on #148
  by @nils-borrmann-tacto
- `getpass.getuser()` raises `OSError` instead of `KeyError` since Python
  3.13, breaking the handshake where no user name can be determined (e.g. in
  containers). Based on #156 by @shsailaubay
- `DateTime64` was decoded as an unsigned integer: pre-1970 values were
  silently corrupted on read and failed on write; the wire value is a signed
  Int64 tick count
- `str()`/`f"{...}"` of status/scheme enums returned e.g.
  `ConnectionStatus.opened` instead of `opened` on Python 3.11+
- `Cursor.fetchone` in streaming mode no longer swallows server errors
  arriving mid-stream
- The never-implemented `use_numpy` setting now emits a `DeprecationWarning`
- Python 2 compat shim (`proto/utils/compat.py`) removed

## 0.3

### 0.3.2

- Restore %(param)s style param substition support. By @baconfield in #147

### 0.3.1

- Fix params substitution for select queries. By @dmkulazhenko in #141.

### 0.3.0

- Update the `Connection` and `Pool` classes API. By @stankudrow in #130:
  - remove the deprecated `connected` property from the `Connection` class
  - fix type hinting for `Cursor` class as incoming parameter for the connection `cursor` method
  - make the connection `close` async method more consistent
  - remove the `asynch/connection.py::connect` function
  - get rid of inheritance from the `asyncio.AbstractServer` for the `Pool` class (mypy is satisfied)
  - check the freshness of a connection before giving it from a pool (inspired by the issue #127 from @nils-borrmann-tacto).
  - remove the`asynch/pool.py::create_pool` function
- Move to poetry>=2.1. By @stankudrow in #133.
- Add `mypy` dependency. By @stankudrow in #128.
- Gracefully handle connections terminated by the server. By @nils-borrmann-tacto in #129.
- Remove the deprecated API from `cursor.py` module. By @stankudrow in #125.
- Remove the deprecated `Pool` API. By @stankudrow in #120.
- Allow requesting more connections from a `Pool` object without raising AsynchPoolError("no free connections"). The issue #121 by @itssimon. By @stankudrow in #124.

## 0.2

### 0.2.5

- Add more validation rules in the `parse_dsn` function. By @stankudrow in #113
- Reconsider the API of the `Connection`, `Cursor` and `Pool` classes and deprecate outdated methods or properties. Define the DB-API v2.0 compliant exception hierarchy. Update project dependencies and metadata. By @stankudrow in #111.
- Fix infinite iteration case when a cursor object is put in the `async for` loop (the discussion #100 by @KuzenkovAG). By @stankudrow in #112.
- Fix pool connection management (the discussion #108 by @DFilyushin) by @stankudrow in #109:

  - add the asynchronous context manager support to the `Pool` class with the pool "startup()" as `__aenter__` and "shutdown()" as `__aexit__` methods;
  - enrich the `Pool` class with the "connection()" method returning an asynchronous context manager responsible for acquiring connections from a pool object and releasing them back into the pool;
  - refactor the `Connection` and `Pool` classes.
- Add the asynchronous context manager support to the `Connection` class. By @stankudrow in #107.
- Make Python3.9 the minimum supported version. Update the project dependencies, metadata, tests. By @stankudrow in #106.

### 0.2.4

- Reset connection state. By @boolka in #101.
- Add lazy date_lut, similar to clickhouse-driver. By @DaniilAnichin in #99.
- Correct check life connection (#71). By @gnomeby in #98.
- Use maxsize for pool connections (#68). By @gnomeby in #97.
- Add Date32 column (#95). By @cortelf in #96.
- Eliminate `IndexError` cases from the `BufferedReader` class methods when reading from an empty buffer. By @stankudrow in #94.
- Fix a bytearray index out of range error while reading a string. By @pufit in #90.
- Make a connection be closed for `ExecuteContext` manager class. By @KPull in #82.
- Add connection validity check in `acquire` method. By @lxneng in #81.

### 0.2.3

- Support json column. (#73)
- Fix connection with `secure=True` and `verify=False`.
- Fix compression.
- Fix exception `Cannot set verify_mode to CERT_NONE when check_hostname is enabled`.

### 0.2.2

- Add `Int128Column`, `Int256Column`, `UInt128Column`, `UInt256Column`, `Decimal256Column`. (#57)
- Add Geo type support. (#56)
- Add decimals in map support. (#55)
- Add `NestedColumn`. (#54)
- Add execution_options support. (#53)
- Fix `IPv6Column`. (#52)
- Fix execution context exception handling. (#51)
- Fix stream_mode. (#44)
- Fix `SimpleAggregateFunction` for nested. (#41)

### 0.2.1

- Fix ping message for unstable network. (#48)

### 0.2.0

- Fix compression not working. (#36)
- Add `BoolColumn`. (#38)

## 0.1

### 0.1.9

- Fix LowCardinalityColumn keys column exception. (#17)

### 0.1.8

- Fix bug in protocol for `FixedString`

### 0.1.7

- Fix bug with `FixedString`

### 0.1.6

- Fix syntax error

### 0.1.5

- Fix syntax error
- Fix `BufferReader.read_bytes`

### 0.1.4

- Fix bugs with `TupleColumn`

### 0.1.3

- Fix bugs with `ArrayColumn` and `LowCardinalityColumn`.

### 0.1.2

- Fix exception and read data bugs.

### 0.1.1

- Add connect pool.

### 0.1.0

- Release first version.
