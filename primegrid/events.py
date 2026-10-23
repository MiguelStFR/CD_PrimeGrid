"""Uma linha JSON por evento relevante; nunca registra cada primo."""

import json
import logging
from datetime import datetime, timezone


def event(node_id: int, name: str, **fields) -> None:
    logging.getLogger("primegrid").info(json.dumps({
        "time": datetime.now(timezone.utc).isoformat(), "node_id": node_id,
        "event": name, **fields,
    }))
