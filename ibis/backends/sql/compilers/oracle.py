from __future__ import annotations

import sqlglot as sg
import sqlglot.expressions as sge
import toolz

import ibis.common.exceptions as com
import ibis.expr.operations as ops
from ibis.backends.sql.compilers.base import NULL, STAR, AggGen, SQLGlotCompiler
from ibis.backends.sql.datatypes import OracleType
from ibis.backends.sql.dialects import Oracle
from ibis.backends.sql.rewrites import (
    FirstValue,
    LastValue,
    exclude_unsupported_window_frame_from_ops,
    exclude_unsupported_window_frame_from_row_number,
    lower_log2,
    lower_log10,
    lower_sample,
    rewrite_empty_order_by_window,
    split_select_distinct_with_order_by,
)


class OracleCompiler(SQLGlotCompiler):
    __slots__ = ()

    agg = AggGen(supports_order_by=True)

    dialect = Oracle
    type_mapper = OracleType
    rewrites = (
        exclude_unsupported_window_frame_from_row_number,
        exclude_unsupported_window_frame_from_ops,
        rewrite_empty_order_by_window,
        *SQLGlotCompiler.rewrites,
    )

    post_rewrites = (split_select_distinct_with_order_by,)

    NAN = sge.Literal.number("binary_double_nan")
    """Backend's NaN literal."""

    POS_INF = sge.Literal.number("binary_double_infinity")
    """Backend's positive infinity literal."""

    NEG_INF = sge.Literal.number("-binary_double_infinity")
    """Backend's negative infinity literal."""

    LOWERED_OPS = {
        ops.Log2: lower_log2,
        ops.Log10: lower_log10,
        ops.Sample: lower_sample(physical_tables_only=True),
    }

    UNSUPPORTED_OPS = (
        ops.Array,
        ops.ArrayFlatten,
        ops.ArrayMap,
        ops.ArrayStringJoin,
        ops.ArgMax,
        ops.ArgMin,
        ops.MultiQuantile,
        ops.RegexSplit,
        ops.StringSplit,
        ops.TimeTruncate,
        ops.Bucket,
        ops.Kurtosis,
        ops.TimestampBucket,
        ops.TimeDelta,
        ops.TimestampDelta,
        ops.TimestampFromYMDHMS,
        ops.TimeFromHMS,
        ops.DayOfWeekIndex,
        ops.DayOfWeekName,
        ops.DateDiff,
        ops.ExtractEpochSeconds,
        ops.ExtractWeekOfYear,
        ops.ExtractDayOfYear,
        ops.RowID,
        ops.RandomUUID,
        ops.StringToTime,
    )

    SIMPLE_OPS = {
        ops.ApproxCountDistinct: "approx_count_distinct",
        ops.BitAnd: "bit_and_agg",
        ops.BitOr: "bit_or_agg",
        ops.BitXor: "bit_xor_agg",
        ops.BitwiseAnd: "bitand",
        ops.Hash: "ora_hash",
        ops.StringAscii: "ascii",
        ops.Mode: "stats_mode",
    }

    @staticmethod
    def _generate_groups(groups):
        return groups

    def visit_Equals(self, op, *, left, right):
        # Oracle didn't have proper boolean types until recently and we handle them
        # as integers so we end up with things like "t0"."bool_col" = 1 (for True)
        # but then if we are testing that a boolean column IS True, it gets rendered as
        # "t0"."bool_col" = 1 = 1
        # so intercept that and change it to WHERE (bool_col = 1)
        # TODO(gil): there must be a better way to do this
        if op.dtype.is_boolean() and isinstance(right, sge.Boolean):
            if right.this:
                return left
            else:
                return sg.not_(left)
        return super().visit_Equals(op, left=left, right=right)

    def visit_IsNull(self, op, *, arg):
        # TODO(gil): find a better way to handle this
        # but CASE WHEN (bool_col = 1) IS NULL isn't valid and we can simply check if
        # bool_col is null
        if isinstance(arg, sge.EQ):
            return arg.this.is_(NULL)
        return arg.is_(NULL)

    def visit_Literal(self, op, *, value, dtype):
        # avoid casting NULL -- oracle handling for these casts is... complicated
        if value is None:
            return NULL
        elif dtype.is_timestamp() or dtype.is_time():
            if getattr(dtype, "timezone", None) is not None:
                return self.f.to_timestamp_tz(
                    value.isoformat(), 'YYYY-MM-DD"T"HH24:MI:SS.FF6TZH:TZM'
                )
            else:
                return self.f.to_timestamp(
                    value.isoformat(), 'YYYY-MM-DD"T"HH24:MI:SS.FF6'
                )
        elif dtype.is_date():
            return self.f.to_date(
                f"{value.year:04d}-{value.month:02d}-{value.day:02d}", "FXYYYY-MM-DD"
            )
        elif dtype.is_uuid():
            return sge.convert(str(value))
        elif dtype.is_interval():
            return self._value_to_interval(value, dtype.unit)

        return super().visit_Literal(op, value=value, dtype=dtype)

    def _value_to_interval(self, arg, unit):
        short = unit.short

        if short in ("Y", "M"):
            return self.f.numtoyminterval(arg, unit.singular)
        elif short in ("D", "h", "m", "s"):
            return self.f.numtodsinterval(arg, unit.singular)
        elif short == "ms":
            return self.f.numtodsinterval(arg / 1e3, "second")
        elif short in "us":
            return self.f.numtodsinterval(arg / 1e6, "second")
        elif short in "ns":
            return self.f.numtodsinterval(arg / 1e9, "second")
        else:
            raise com.UnsupportedArgumentError(
                f"Interval {unit.name} not supported by Oracle"
            )

    def visit_Cast(self, op, *, arg, to):
        from_ = op.arg.dtype
        if from_.is_numeric() and to.is_interval():
            # CASTing to an INTERVAL in Oracle requires specifying digits of
            # precision that are a pain.  There are two helper functions that
            # should be used instead.
            return self._value_to_interval(arg, to.unit)
        elif from_.is_string() and to.is_date():
            return self.f.to_date(arg, "FXYYYY-MM-DD")

        # casting anything to NOT NULL doesn't make sense, since nullability in
        # SQL databases is a constraint on physical tables, not on the
        # operations on the data
        return self.cast(arg, to.copy(nullable=True))

    def visit_Limit(self, op, *, parent, n, offset):
        # push limit/offset into subqueries
        if isinstance(parent, sge.Subquery) and parent.this.args.get("limit") is None:
            result = parent.this
            alias = parent.alias
        else:
            result = sg.select(STAR).from_(parent)
            alias = None

        if isinstance(n, int):
            result = result.limit(n)
        elif n is not None:
            raise com.UnsupportedArgumentError(
                "No support for dynamic limit in the Oracle backend."
            )
            # TODO: re-enable this for dynamic limits
            # but it should be paired with offsets working
            # result = result.where(C.ROWNUM <= sg.select(n).from_(parent).subquery())
        else:
            assert n is None, n
            if self.no_limit_value is not None:
                result = result.limit(self.no_limit_value)

        assert offset is not None, "offset is None"

        if offset > 0:
            raise com.UnsupportedArgumentError(
                "No support for limit offsets in the Oracle backend."
            )

        if alias is not None:
            return result.subquery(alias)
        return result

    def visit_Date(self, op, *, arg):
        return self.f.trunc(arg, "DDD")

    def visit_IsNan(self, op, *, arg):
        return arg.eq(self.NAN)

    def visit_Log(self, op, *, arg, base):
        return self.f.log(base, arg)

    def visit_IsInf(self, op, *, arg):
        return arg.isin(self.POS_INF, self.NEG_INF)

    def visit_RandomScalar(self, op):
        # Not using FuncGen here because of dotted function call
        return sg.func("dbms_random.value")

    def visit_Pi(self, op):
        return self.f.acos(-1)

    def visit_Cot(self, op, *, arg):
        return 1 / self.f.tan(arg)

    def visit_Degrees(self, op, *, arg):
        return 180 * arg / self.visit_node(ops.Pi())

    def visit_Radians(self, op, *, arg):
        return self.visit_node(ops.Pi()) * arg / 180

    def visit_Modulus(self, op, *, left, right):
        return self.f.anon.mod(left, right)

    def visit_Levenshtein(self, op, *, left, right):
        # Not using FuncGen here because of dotted function call
        return sg.func("utl_match.edit_distance", left, right)

    def visit_StartsWith(self, op, *, arg, start):
        return self.f.substr(arg, 0, self.f.length(start)).eq(start)

    def visit_EndsWith(self, op, *, arg, end):
        return self.f.substr(arg, -1 * self.f.length(end), self.f.length(end)).eq(end)

    def visit_StringFind(self, op, *, arg, substr, start, end):
        if end is not None:
            raise NotImplementedError("`end` is not implemented")

        sub_string = substr

        if start is not None:
            arg = self.f.substr(arg, start + 1)
            pos = self.f.instr(arg, sub_string)
            # TODO(gil): why, oh why, does this need an extra +1 on the end?
            return sg.case().when(pos > 0, pos - 1 + start).else_(-1) + 1

        return self.f.instr(arg, sub_string)

    def visit_StrRight(self, op, *, arg, nchars):
        return self.f.substr(arg, -nchars)

    def visit_RegexExtract(self, op, *, arg, pattern, index):
        return self.if_(
            index.eq(0),
            self.f.regexp_substr(arg, pattern),
            self.f.regexp_substr(arg, pattern, 1, 1, "cn", index),
        )

    def visit_RegexReplace(self, op, *, arg, pattern, replacement):
        return sge.RegexpReplace(this=arg, expression=pattern, replacement=replacement)

    def visit_StringContains(self, op, *, haystack, needle):
        return self.f.instr(haystack, needle) > 0

    def visit_StringJoin(self, op, *, arg, sep):
        return self.f.concat(*toolz.interpose(sep, arg))

    def visit_LPad(self, op, *, arg, length, pad):
        return self.f.lpad(arg, self.f.greatest(self.f.length(arg), length), pad)

    def visit_RPad(self, op, *, arg, length, pad):
        return self.f.rpad(arg, self.f.greatest(self.f.length(arg), length), pad)

    ## Aggregate stuff

    def visit_Correlation(self, op, *, left, right, where, how):
        if how == "sample":
            raise ValueError(
                "Oracle only implements population correlation coefficient"
            )
        return self.agg.corr(left, right, where=where)

    def visit_Covariance(self, op, *, left, right, where, how):
        if how == "sample":
            return self.agg.covar_samp(left, right, where=where)
        return self.agg.covar_pop(left, right, where=where)

    def visit_ApproxMedian(self, op, *, arg, where):
        return self.visit_Quantile(op, arg=arg, quantile=0.5, where=where)

    def visit_Quantile(self, op, *, arg, quantile, where):
        suffix = "cont" if op.arg.dtype.is_numeric() else "disc"
        funcname = f"percentile_{suffix}"

        if where is not None:
            arg = self.if_(where, arg)

        expr = sge.WithinGroup(
            this=self.f[funcname](quantile),
            expression=sge.Order(expressions=[sge.Ordered(this=arg)]),
        )
        return expr

    def visit_ApproxQuantile(self, op, *, arg, quantile, where):
        if where is not None:
            arg = self.if_(where, arg)

        return sge.WithinGroup(
            this=self.f.approx_percentile(quantile),
            expression=sge.Order(expressions=[sge.Ordered(this=arg)]),
        )

    def visit_CountDistinct(self, op, *, arg, where):
        if where is not None:
            arg = self.if_(where, arg)

        return sge.Count(this=sge.Distinct(expressions=[arg]))

    def visit_CountStar(self, op, *, arg, where):
        if where is not None:
            return self.f.count(self.if_(where, 1, NULL))
        return self.f.count(STAR)

    def visit_IdenticalTo(self, op, *, left, right):
        # sqlglot NullSafeEQ uses "is not distinct from" which isn't supported in oracle
        return (
            sg.case()
            .when(left.eq(right).or_(left.is_(NULL).and_(right.is_(NULL))), 0)
            .else_(1)
            .eq(0)
        )

    def visit_Xor(self, op, *, left, right):
        return (left.or_(right)).and_(sg.not_(left.and_(right)))

    def visit_DateTruncate(self, op, *, arg, unit):
        trunc_unit_mapping = {
            "Y": "year",
            "Q": "Q",
            "M": "MONTH",
            "W": "IW",
            "D": "DDD",
            "h": "HH",
            "m": "MI",
        }

        timestamp_unit_mapping = {
            "s": "SS",
            "ms": "SS.FF3",
            "us": "SS.FF6",
            "ns": "SS.FF9",
        }

        if (unyt := timestamp_unit_mapping.get(unit.short)) is not None:
            # Oracle only has trunc(DATE) and that can't do sub-minute precision, but we can
            # handle those separately.
            return self.f.to_timestamp(
                self.f.to_char(arg, f"YYYY-MM-DD HH24:MI:{unyt}"),
                f"YYYY-MM-DD HH24:MI:{unyt}",
            )

        if (unyt := trunc_unit_mapping.get(unit.short)) is None:
            raise com.UnsupportedOperationError(f"Unsupported truncate unit {unit}")

        return self.f.anon.trunc(arg, unyt)

    visit_TimestampTruncate = visit_DateTruncate

    def visit_WindowFunction(self, op, *, how, func, start, end, group_by, order_by):
        # Oracle has two (more?) types of analytic functions you can use inside OVER.
        #
        # The first group accepts an "analytic clause" which is decomposed into the
        # PARTITION BY, ORDER BY and the windowing clause (e.g. ROWS BETWEEN
        # UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING).  These are the "full" window functions.
        #
        # The second group accepts an _optional_ PARTITION BY clause and a _required_ ORDER BY clause.
        # If you try to pass, for instance, LEAD(col, 1) OVER() AS "val", this will error.
        #
        # The list of functions which accept the full analytic clause (and so
        # accept a windowing clause) are those functions which are marked with
        # an asterisk at the bottom of this page (yes, Oracle thinks this is
        # a reasonable way to demarcate them):
        # https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/Analytic-Functions.html
        #
        # (Side note: these unordered window function queries were not erroring
        # in the SQLAlchemy Oracle backend but they were raising AssertionErrors.
        # This is because the SQLAlchemy Oracle dialect automatically inserts an
        # ORDER BY whether you ask it to or not.)
        #
        # If the windowing clause is omitted, the default is
        # RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        #
        # I (@gforsyth) believe that this is the windowing range applied to the
        # analytic functions (like LEAD, LAG, CUME_DIST) which don't allow
        # specifying a windowing clause.
        #
        # This allowance for specifying a windowing clause is handled below by
        # explicitly listing the ops which correspond to the analytic functions
        # that accept it.

        if type(op.func) in (
            # TODO: figure out REGR_* functions and also manage this list better
            # Allowed windowing clause functions
            ops.Mean,  # "avg",
            ops.Correlation,  # "corr",
            ops.Count,  # "count",
            ops.Covariance,  # "covar_pop", "covar_samp",
            FirstValue,  # "first_value",
            LastValue,  # "last_value",
            ops.Max,  # "max",
            ops.Min,  # "min",
            ops.NthValue,  # "nth_value",
            ops.StandardDev,  # "stddev","stddev_pop","stddev_samp",
            ops.Sum,  # "sum",
            ops.Variance,  # "var_pop","var_samp","variance",
        ):
            if start is None:
                start = {}
            if end is None:
                end = {}

            start_value = start.get("value", "UNBOUNDED")
            start_side = start.get("side", "PRECEDING")
            end_value = end.get("value", "UNBOUNDED")
            end_side = end.get("side", "FOLLOWING")

            spec = sge.WindowSpec(
                kind=how.upper(),
                start=start_value,
                start_side=start_side,
                end=end_value,
                end_side=end_side,
                over="OVER",
            )
        elif not order_by:
            # For other analytic functions, ORDER BY is required
            raise com.UnsupportedOperationError(
                f"Function {op.func.name} cannot be used in Oracle without an order_by."
            )
        else:
            # and no windowing clause is supported, so set the spec to None.
            spec = None

        order = sge.Order(expressions=order_by) if order_by else None

        spec = self._minimize_spec(op, spec)

        return sge.Window(this=func, partition_by=group_by, order=order, spec=spec)

    def visit_StringConcat(self, op, *, arg):
        any_args_null = (a.is_(NULL) for a in arg)
        return self.if_(sg.or_(*any_args_null), NULL, self.f.concat(*arg))

    def visit_ExtractIsoYear(self, op, *, arg):
        return self.cast(self.f.to_char(arg, "IYYY"), op.dtype)

    def visit_GroupConcat(self, op, *, arg, where, sep, order_by):
        if where is not None:
            arg = self.if_(where, arg, NULL)

        out = self.f.listagg(arg, sep)

        if order_by:
            out = sge.WithinGroup(this=out, expression=sge.Order(expressions=order_by))

        return out

    def visit_IntervalFromInteger(self, op, *, arg, unit):
        return self._value_to_interval(arg, unit)

    def visit_DateFromYMD(self, op, *, year, month, day):
        year = self.f.lpad(year, 4, "0")
        month = self.f.lpad(month, 2, "0")
        day = self.f.lpad(day, 2, "0")
        return self.f.to_date(self.f.concat(year, month, day), "FXYYYYMMDD")

    def visit_DateDelta(self, op, *, left, right, part):
        if not isinstance(part, sge.Literal):
            raise com.UnsupportedOperationError(
                "Only literal `part` values are supported for date delta"
            )
        if part.this != "day":
            raise com.UnsupportedOperationError(
                f"Only 'day' part is supported for date delta in the {self.dialect} backend"
            )
        return left - right

    def visit_Strip(self, op, *, arg):
        # Oracle's `TRIM` only accepts a single character to trim off, unlike
        # Oracle's `RTRIM` and `LTRIM` which accept a set of characters to
        # remove.
        return self.visit_RStrip(op, arg=self.visit_LStrip(op, arg=arg))

    def visit_Round(self, op, *, arg, digits):
        if op.dtype.is_integer():
            return self.f.round(arg)
        return self.cast(self.f.round(arg, digits), op.dtype)


compiler = OracleCompiler()
