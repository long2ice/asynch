"""ClickHouse `JSON` column (24.8+ binary format).

Reading composes DynamicColumn (a SerializationVariant per path) with an
`Array(Tuple(String, String))` shared sub-column; writing emits the V2
object framing.
"""

import pytest

from asynch.cursors import DictCursor
from asynch.errors import ServerException


@pytest.fixture(autouse=True)
async def require_json_type(conn):
    """Skip where the server has no usable JSON type.

    It was experimental before 25.x and is rejected outright there, so these
    tests describe the driver against servers that actually speak the format.
    """
    async with conn.cursor() as cursor:
        try:
            await cursor.execute("SELECT '{}'::JSON")
        except ServerException as e:
            pytest.skip(f"server has no usable JSON type: {e}")


DOCUMENTS = [
    {"a": 1, "b": "x"},
    {"nested": {"deep": {"deeper": "v"}}},
    {"nums": [1, 2, 3], "strs": ["p", "q"]},
    {"flag": True, "ratio": 2.5},
    {},
    {"big": 9223372036854775807},
    {"unicode": "中文 éà ñ"},
    {"empty_list": []},
    {"mix": {"n": 1, "s": "t", "b": False, "arr": [1, 2]}},
]


@pytest.fixture()
async def json_table(conn):
    async with conn.cursor() as cursor:
        await cursor.execute("DROP TABLE IF EXISTS test.json_column")
        await cursor.execute(
            "CREATE TABLE test.json_column (id UInt32, doc JSON) ENGINE = MergeTree ORDER BY id"
        )
    try:
        yield "test.json_column"
    finally:
        async with conn.cursor() as cursor:
            await cursor.execute("DROP TABLE IF EXISTS test.json_column")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("literal", "expected"),
    [
        ('{"a": 1, "b": "x"}', {"a": 1, "b": "x"}),
        ('{"a": {"b": {"c": "deep"}}}', {"a": {"b": {"c": "deep"}}}),
        ('{"i": 42, "f": 3.5, "b": true}', {"i": 42, "f": 3.5, "b": True}),
        ('{"n": null}', {}),
        ("{}", {}),
        ('{"nums": [1, 2, 3]}', {"nums": [1, 2, 3]}),
        ('{"m": [[1, 2], [3]]}', {"m": [[1, 2], [3]]}),
        ('{"items": [{"id": 1}, {"id": 2}]}', {"items": [{"id": 1}, {"id": 2}]}),
        ('{"a": [1, null, 3]}', {"a": [1, None, 3]}),
        ('{"big": 9223372036854775807}', {"big": 9223372036854775807}),
    ],
)
async def test_read_json_literal(conn, literal, expected):
    """The server builds the JSON; the driver only has to decode it."""
    async with conn.cursor() as cursor:
        await cursor.execute(f"SELECT {literal!r}::JSON AS j")
        assert (await cursor.fetchone())[0] == expected


@pytest.mark.asyncio
async def test_read_heterogeneous_array(conn):
    """A mixed-type array decodes to whatever variant the server picked.

    Servers differ here: some keep Int64/String/Bool as separate variants,
    others coerce the whole array to String. Both are correct on the wire, so
    this pins the shape rather than the exact types.
    """
    async with conn.cursor() as cursor:
        await cursor.execute("""SELECT '{"mixed": [1, "a", true]}'::JSON AS j""")
        value = (await cursor.fetchone())[0]

    assert set(value) == {"mixed"}
    assert value["mixed"] in ([1, "a", True], ["1", "a", "true"])


@pytest.mark.asyncio
async def test_json_roundtrip(conn, json_table):
    async with conn.cursor() as cursor:
        await cursor.execute(
            f"INSERT INTO {json_table} (id, doc) VALUES",
            [(i, doc) for i, doc in enumerate(DOCUMENTS)],
        )
        await cursor.execute(f"SELECT id, doc FROM {json_table} ORDER BY id")
        rows = await cursor.fetchall()

    assert [doc for _, doc in rows] == DOCUMENTS


@pytest.mark.asyncio
async def test_json_accepts_text(conn, json_table):
    """A JSON string is parsed, so both spellings can be mixed in one block."""
    async with conn.cursor() as cursor:
        await cursor.execute(
            f"INSERT INTO {json_table} (id, doc) VALUES",
            [(1, '{"from": "text"}'), (2, {"from": "dict"})],
        )
        await cursor.execute(f"SELECT doc FROM {json_table} ORDER BY id")
        assert [row[0] for row in await cursor.fetchall()] == [
            {"from": "text"},
            {"from": "dict"},
        ]


@pytest.mark.asyncio
async def test_json_large_batch(conn, json_table):
    """Many rows over many paths: exercises the per-path variant grouping."""
    rows = [(i, {f"p{i % 20}": i, "common": "c", "arr": [i, i + 1]}) for i in range(2000)]
    async with conn.cursor() as cursor:
        await cursor.execute(f"INSERT INTO {json_table} (id, doc) VALUES", rows)
        await cursor.execute(f"SELECT count(), countIf(doc.common = 'c') FROM {json_table}")
        assert await cursor.fetchone() == (2000, 2000)

        await cursor.execute(f"SELECT doc FROM {json_table} ORDER BY id LIMIT 1")
        assert (await cursor.fetchone())[0] == {"arr": [0, 1], "common": "c", "p0": 0}


@pytest.mark.asyncio
async def test_json_path_access_after_insert(conn, json_table):
    """What the driver wrote must be addressable by the server's path syntax."""
    async with conn.cursor() as cursor:
        await cursor.execute(f"INSERT INTO {json_table} (id, doc) VALUES", [(1, {"a": {"b": 7}})])
        await cursor.execute(f"SELECT doc.a.b FROM {json_table}")
        assert (await cursor.fetchone())[0] == 7


@pytest.mark.asyncio
async def test_json_with_dict_cursor(conn, json_table):
    async with conn.cursor(cursor=DictCursor) as cursor:
        await cursor.execute(f"INSERT INTO {json_table} (id, doc) VALUES", [(1, {"k": "v"})])
        await cursor.execute(f"SELECT id, doc FROM {json_table}")
        assert await cursor.fetchall() == [{"id": 1, "doc": {"k": "v"}}]
