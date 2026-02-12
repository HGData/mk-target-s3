from datetime import datetime
import math
import json
from decimal import Decimal

from bson import ObjectId

from target_s3.formats.format_base import FormatBase


class JsonSerialize(json.JSONEncoder):
    def default(self, obj: any) -> any:
        if isinstance(obj, ObjectId):
            return str(obj)
        if isinstance(obj, datetime):
            return obj.isoformat()
        elif isinstance(obj, Decimal):
            # Handle Decimal types
            if obj.is_nan():
                return "NaN"
            elif obj.is_infinite():
                return "Infinity" if obj > 0 else "-Infinity"
            else:
                return float(obj)
        elif isinstance(obj, float):
            # Handle float special values
            if math.isnan(obj):
                return "NaN"
            elif math.isinf(obj):
                return "Infinity" if obj > 0 else "-Infinity"
            else:
                return obj
        else:
            return str(obj)


class FormatJsonl(FormatBase):
    def __init__(self, config, context) -> None:
        super().__init__(config, context, "jsonl")
        pass

    def _prepare_records(self):
        # use default behavior, no additional prep needed
        return super()._prepare_records()

    def _write(self) -> None:
        return super()._write('\n'.join(json.dumps(r, cls=JsonSerialize) for r in self.records))

    def run(self) -> None:
        # use default behavior, no additional run steps needed
        return super().run(self.context["records"])
