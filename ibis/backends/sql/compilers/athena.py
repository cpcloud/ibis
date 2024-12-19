from __future__ import annotations

import re

from sqlglot.dialects import Athena

import ibis.expr.operations as ops
from ibis.backends.sql.compilers.base import SQLGlotCompiler
from ibis.backends.sql.datatypes import AthenaType

_NAME_REGEX = re.compile(r'[^!"$()*,./;?@[\\\]^`{}~\n]+')


class AthenaCompiler(SQLGlotCompiler):
    __slots__ = ()
    dialect = Athena
    type_mapper = AthenaType

    SIMPLE_OPS = {
        ops.Divide: "try_divide",
        ops.Mode: "mode",
        ops.BitAnd: "bit_and",
        ops.BitOr: "bit_or",
        ops.BitXor: "bit_xor",
        ops.TypeOf: "typeof",
        ops.RandomUUID: "uuid",
        ops.StringSplit: "split",
    }

    UNSUPPORTED_OPS = (
        ops.ElementWiseVectorizedUDF,
        ops.AnalyticVectorizedUDF,
        ops.ReductionVectorizedUDF,
        ops.RowID,
        ops.TimestampBucket,
    )

    @staticmethod
    def _gen_valid_name(name: str) -> str:
        return "_".join(map(str.strip, _NAME_REGEX.findall(name))) or "tmp"


compiler = AthenaCompiler()
