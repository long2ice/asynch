# ChangeLog

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
