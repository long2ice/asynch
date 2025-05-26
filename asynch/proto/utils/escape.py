from datetime import date, datetime
from enum import Enum
from types import MappingProxyType
from typing import Any, Dict, Mapping
from uuid import UUID

from .compat import string_types, text_type

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
        return "'%s'" % item.strftime("%Y-%m-%d %H:%M:%S")

    elif isinstance(item, date):
        return "'%s'" % item.strftime("%Y-%m-%d")

    elif isinstance(item, string_types):
        return "'%s'" % "".join(escape_chars_map.get(c, c) for c in item)

    elif isinstance(item, list):
        return "[%s]" % ", ".join(text_type(escape_param(x)) for x in item)

    elif isinstance(item, tuple):
        return "(%s)" % ", ".join(text_type(escape_param(x)) for x in item)

    elif isinstance(item, Enum):
        return escape_param(item.value)

    elif isinstance(item, UUID):
        return "'%s'" % str(item)

    else:
        return item


def escape_params(params: Mapping[str, Any]) -> MappingProxyType[str, str]:
    return MappingProxyType({key: escape_param(value) for key, value in params.keys()})


def substitute_params(query: str, params: Mapping) -> str:
    if not isinstance(params, Mapping):
        raise ValueError("Parameters are expected in dict form")

    try:
        escaped = escape_params(params)
        return query.format(**escaped)
    except KeyError as e:
        raise KeyError(f"Parameter {e} not found") from e
