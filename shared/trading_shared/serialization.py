from datetime import datetime
from decimal import Decimal
from uuid import UUID

from sqlalchemy import inspect as sa_inspect


def serialize(obj):
    """Convert any SQLAlchemy model instance to a JSON-safe dict."""
    if obj is None:
        return None
    result = {}
    for attr in sa_inspect(obj).mapper.column_attrs:
        val = getattr(obj, attr.key)
        if isinstance(val, UUID):
            result[attr.key] = str(val)
        elif isinstance(val, datetime):
            result[attr.key] = val.isoformat()
        elif isinstance(val, Decimal):
            result[attr.key] = float(val)
        else:
            result[attr.key] = val
    return result
