from collections.abc import Mapping
from datetime import date, datetime
from enum import Enum
from typing import Any
from uuid import UUID

escape_chars_map = {
    "\b": "\\b",
    "\f": "\\f",
    "\r": "\\r",
    "\n": "\\n",
    "\t": "\\t",
    "\0": "\\0",
    "\a": "\\a",
    "\v": "\\v",
    "\\": "\\\\",
    "'": "\\'",
}


def escape_param(item: Any) -> str:
    if item is None:
        return "NULL"

    elif isinstance(item, datetime):
        return "'{}'".format(item.strftime("%Y-%m-%d %H:%M:%S"))

    elif isinstance(item, date):
        return "'{}'".format(item.strftime("%Y-%m-%d"))

    elif isinstance(item, str):
        return "'{}'".format("".join(escape_chars_map.get(c, c) for c in item))

    elif isinstance(item, list):
        return "[{}]".format(", ".join(escape_param(x) for x in item))

    elif isinstance(item, tuple):
        return "({})".format(", ".join(escape_param(x) for x in item))

    elif isinstance(item, Enum):
        return escape_param(item.value)

    elif isinstance(item, UUID):
        return f"'{str(item)}'"

    else:
        return str(item)


def escape_params(params: Mapping[str, Any]) -> dict[str, str]:
    return {key: escape_param(value) for key, value in params.items()}
