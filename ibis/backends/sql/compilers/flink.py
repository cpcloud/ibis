"""Flink Ibis expression to SQL compiler."""

from __future__ import annotations

import sqlglot as sg
import sqlglot.expressions as sge

import ibis.common.exceptions as com
import ibis.expr.datatypes as dt
import ibis.expr.operations as ops
from ibis.backends.sql.compilers.base import NULL, STAR, AggGen, SQLGlotCompiler
from ibis.backends.sql.datatypes import FlinkType
from ibis.backends.sql.dialects import Flink
from ibis.backends.sql.rewrites import (
    exclude_unsupported_window_frame_from_ops,
    exclude_unsupported_window_frame_from_rank,
    exclude_unsupported_window_frame_from_row_number,
    split_select_distinct_with_order_by,
)


class FlinkAggGen(AggGen):
    def aggregate(self, compiler, name, *args, where=None, order_by=()):
        if order_by:
            raise com.UnsupportedOperationError(
                "ordering of order-sensitive aggregations via `order_by` is "
                "not supported for this backend"
            )

        func = compiler.f[name]
        if where is not None:
            # Flink does support FILTER, but it's broken for:
            #
            # 1. certain aggregates: std/var doesn't return the right result
            # 2. certain kinds of predicates: x IN y doesn't filter the right
            #    values out
            # 3. certain aggregates AND predicates STD(w) FILTER (WHERE x IN Y)
            #    returns an incorrect result
            #
            # One solution is to try `IF(predicate, arg, NULL)`.
            #
            # Unfortunately that won't work without casting the NULL to a
            # specific type.
            #
            # At this point in the Ibis compiler we don't have any of the Ibis
            # operation's type information because we thrown it away. In every
            # other engine Ibis supports the type of a NULL literal is inferred
            # by the engine.
            #
            # Using a CASE statement and leaving out the explicit NULL does the
            # trick for Flink.
            args = tuple(sge.Case(ifs=[sge.If(this=where, true=arg)]) for arg in args)
        return func(*args)


class FlinkCompiler(SQLGlotCompiler):
    quoted = True
    dialect = Flink
    type_mapper = FlinkType

    agg = FlinkAggGen()

    rewrites = (
        exclude_unsupported_window_frame_from_row_number,
        exclude_unsupported_window_frame_from_ops,
        exclude_unsupported_window_frame_from_rank,
        *SQLGlotCompiler.rewrites,
    )
    post_rewrites = (split_select_distinct_with_order_by,)

    UNSUPPORTED_OPS = (
        ops.AnalyticVectorizedUDF,
        ops.ApproxMedian,
        ops.ArrayFlatten,
        ops.ArrayStringJoin,
        ops.ArgMax,
        ops.ArgMin,
        ops.Correlation,
        ops.CountDistinctStar,
        ops.Covariance,
        ops.DateDiff,
        ops.FindInSet,
        ops.IsInf,
        ops.IsNan,
        ops.Levenshtein,
        ops.Median,
        ops.NthValue,
        ops.ReductionVectorizedUDF,
        ops.RegexSplit,
        ops.RowID,
        ops.Translate,
        ops.StringToTime,
        ops.Kurtosis,
    )

    SIMPLE_OPS = {
        ops.All: "min",
        ops.Any: "max",
        ops.ApproxCountDistinct: "approx_count_distinct",
        ops.ArrayDistinct: "array_distinct",
        ops.ArrayLength: "cardinality",
        ops.ArrayPosition: "array_position",
        ops.ArrayRemove: "array_remove",
        ops.ArraySort: "array_sort",
        ops.ArrayUnion: "array_union",
        ops.ExtractDayOfYear: "dayofyear",
        ops.MapKeys: "map_keys",
        ops.MapValues: "map_values",
        ops.Power: "power",
        ops.RegexSearch: "regexp",
        ops.StrRight: "right",
        ops.StringLength: "char_length",
        ops.StringToDate: "to_date",
        ops.StringToTimestamp: "to_timestamp",
        ops.TypeOf: "typeof",
    }

    @property
    def NAN(self):
        raise NotImplementedError("Flink does not support NaN")

    @property
    def POS_INF(self):
        raise NotImplementedError("Flink does not support Infinity")

    NEG_INF = POS_INF

    @staticmethod
    def _generate_groups(groups):
        return groups

    @staticmethod
    def _minimize_spec(op, spec):
        if (
            op.start is None
            and isinstance(getattr(end := op.end, "value", None), ops.Literal)
            and end.value.value == 0
            and end.following
        ):
            return None
        elif (
            isinstance(getattr(end := op.end, "value", None), ops.Cast)
            and end.value.arg.value == 0
            and end.following
        ):
            spec.args["end"] = "CURRENT ROW"
            spec.args["end_side"] = None
        return spec

    def visit_WindowAggregate(
        self,
        op,
        *,
        parent,
        window_type,
        time_col,
        groups,
        metrics,
        window_size,
        window_slide,
        window_offset,
    ):
        if window_type == "tumble":
            assert window_slide is None

        args = [
            self.v[f"TABLE {parent.this.sql(self.dialect)}"],
            # `time_col` has the table _alias_, instead of the table, but it is
            # required to be bound to the table, this happens because of the
            # way we construct the op in the tumble API using bind
            #
            # perhaps there's a better way to deal with this
            self.f.descriptor(time_col.this),
            window_slide,
            window_size,
            window_offset,
        ]

        window_func = getattr(self.f, window_type)

        # create column references to new columns generated by Flink in the output
        window_start = sg.column(
            "window_start", table=parent.alias_or_name, quoted=True
        )
        window_end = sg.column("window_end", table=parent.alias_or_name, quoted=True)

        return (
            sg.select(
                window_start,
                window_end,
                *self._cleanup_names(groups),
                *self._cleanup_names(metrics),
                copy=False,
            )
            .from_(
                self.f.table(window_func(*filter(None, args))).as_(
                    parent.alias_or_name, quoted=True
                )
            )
            .group_by(
                *self._generate_groups([window_start, window_end, *groups.values()])
            )
        )

    def visit_InMemoryTable(self, op, *, name, schema, data):
        # the performance of this is rather terrible
        tuples = data.to_frame().itertuples(index=False)
        quoted = self.quoted
        columns = [sg.column(col, quoted=quoted) for col in schema.names]
        alias = sge.TableAlias(
            this=sg.to_identifier(name, quoted=quoted), columns=columns
        )
        expressions = [
            sge.Tuple(
                expressions=[
                    self.visit_Literal(
                        ops.Literal(col, dtype=dtype), value=col, dtype=dtype
                    )
                    for col, dtype in zip(row, schema.types)
                ]
            )
            for row in tuples
        ]

        expr = sge.Values(expressions=expressions, alias=alias)
        return sg.select(*columns).from_(expr)

    def visit_NonNullLiteral(self, op, *, value, dtype):
        if dtype.is_binary():
            # TODO: is this decode safe?
            return self.cast(value.decode(), dtype)
        elif dtype.is_uuid():
            return sge.convert(str(value))
        elif dtype.is_array():
            value_type = dtype.value_type
            result = self.f.array(
                *(
                    self.visit_Literal(
                        ops.Literal(v, dtype=value_type), value=v, dtype=value_type
                    )
                    for v in value
                )
            )
            if value:
                return result
            return sge.Cast(this=result, to=self.type_mapper.from_ibis(dtype))
        elif dtype.is_map():
            key_type = dtype.key_type
            value_type = dtype.value_type
            keys = self.f.array(
                *(
                    self.visit_Literal(
                        ops.Literal(v, dtype=key_type), value=v, dtype=key_type
                    )
                    for v in value.keys()
                )
            )
            values = self.f.array(
                *(
                    self.visit_Literal(
                        ops.Literal(v, dtype=value_type), value=v, dtype=value_type
                    )
                    for v in value.values()
                )
            )
            return self.cast(self.f.map_from_arrays(keys, values), dtype)
        elif dtype.is_timestamp():
            return self.cast(
                value.replace(tzinfo=None).isoformat(sep=" ", timespec="microseconds"),
                dtype,
            )
        elif dtype.is_date():
            return self.cast(value.isoformat(), dtype)
        elif dtype.is_time():
            return self.cast(value.isoformat(timespec="microseconds"), dtype)
        return None

    def visit_ArrayIndex(self, op, *, arg, index):
        return sge.Bracket(this=arg, expressions=[index + 1])

    def visit_Xor(self, op, *, left, right):
        return sg.or_(sg.and_(left, sg.not_(right)), sg.and_(sg.not_(left), right))

    def visit_Literal(self, op, *, value, dtype):
        if value is None:
            assert dtype.nullable, "dtype is not nullable but value is None"
            if not dtype.is_null():
                return self.cast(NULL, dtype)
            return NULL
        return super().visit_Literal(op, value=value, dtype=dtype)

    def visit_MapGet(self, op, *, arg, key, default):
        if default is NULL:
            default = self.cast(default, op.dtype)
        return self.f.coalesce(arg[self.cast(key, op.arg.dtype.key_type)], default)

    def visit_ArraySlice(self, op, *, arg, start, stop):
        args = [arg, self.if_(start >= 0, start + 1, start)]

        if stop is not None:
            args.append(
                self.if_(stop >= 0, stop, self.f.cardinality(arg) - self.f.abs(stop))
            )

        return self.f.array_slice(*args)

    def visit_First(self, op, *, arg, where, order_by, include_null):
        if include_null:
            raise com.UnsupportedOperationError(
                "`include_null=True` is not supported by the flink backend"
            )
        return self.agg.first_value(arg, where=where, order_by=order_by)

    def visit_Last(self, op, *, arg, where, order_by, include_null):
        if include_null:
            raise com.UnsupportedOperationError(
                "`include_null=True` is not supported by the flink backend"
            )
        return self.agg.last_value(arg, where=where, order_by=order_by)

    def visit_Not(self, op, *, arg):
        return sg.not_(self.cast(arg, dt.boolean))

    def visit_Date(self, op, *, arg):
        return self.cast(arg, dt.date)

    def visit_TryCast(self, op, *, arg, to):
        type_mapper = self.type_mapper
        if op.arg.dtype.is_temporal() and to.is_numeric():
            return self.f.unix_timestamp(
                sge.TryCast(this=arg, to=type_mapper.from_ibis(dt.string))
            )
        return sge.TryCast(this=arg, to=type_mapper.from_ibis(to))

    def visit_Divide(self, op, *, left, right):
        dtype = op.dtype
        return self.cast(left, dtype) / self.cast(right, dtype)

    def visit_FloorDivide(self, op, *, left, right):
        return self.f.floor(sge.paren(left) / sge.paren(right))

    def visit_JSONGetItem(self, op, *, arg, index):
        assert isinstance(op.index, ops.Literal)
        idx = op.index
        val = idx.value
        if idx.dtype.is_integer():
            query_path = f"$[{val}]"
        else:
            assert idx.dtype.is_string(), idx.dtype
            query_path = f"$.{val}"

        key_hack = f"{sge.convert(query_path).sql(self.dialect)} WITH CONDITIONAL ARRAY WRAPPER"
        return self.f.json_query(arg, self.v[key_hack])

    def visit_TimestampFromUNIX(self, op, *, arg, unit):
        from ibis.common.temporal import TimestampUnit

        if unit == TimestampUnit.MILLISECOND:
            precision = 3
        elif unit == TimestampUnit.SECOND:
            precision = 0
        else:
            raise ValueError(f"{unit!r} unit is not supported!")

        return self.cast(self.f.to_timestamp_ltz(arg, precision), dt.timestamp)

    def visit_Time(self, op, *, arg):
        return self.cast(arg, op.dtype)

    def visit_TimeFromHMS(self, op, *, hours, minutes, seconds):
        padded_hour = self.f.lpad(self.cast(hours, dt.string), 2, "0")
        padded_minute = self.f.lpad(self.cast(minutes, dt.string), 2, "0")
        padded_second = self.f.lpad(self.cast(seconds, dt.string), 2, "0")
        return self.cast(
            self.f.concat(padded_hour, ":", padded_minute, ":", padded_second), op.dtype
        )

    def visit_DateFromYMD(self, op, *, year, month, day):
        padded_year = self.f.lpad(self.cast(year, dt.string), 4, "0")
        padded_month = self.f.lpad(self.cast(month, dt.string), 2, "0")
        padded_day = self.f.lpad(self.cast(day, dt.string), 2, "0")
        return self.cast(
            self.f.concat(padded_year, "-", padded_month, "-", padded_day), op.dtype
        )

    def visit_TimestampFromYMDHMS(
        self, op, *, year, month, day, hours, minutes, seconds
    ):
        padded_year = self.f.lpad(self.cast(year, dt.string), 4, "0")
        padded_month = self.f.lpad(self.cast(month, dt.string), 2, "0")
        padded_day = self.f.lpad(self.cast(day, dt.string), 2, "0")
        padded_hour = self.f.lpad(self.cast(hours, dt.string), 2, "0")
        padded_minute = self.f.lpad(self.cast(minutes, dt.string), 2, "0")
        padded_second = self.f.lpad(self.cast(seconds, dt.string), 2, "0")
        return self.cast(
            self.f.concat(
                padded_year,
                "-",
                padded_month,
                "-",
                padded_day,
                " ",
                padded_hour,
                ":",
                padded_minute,
                ":",
                padded_second,
            ),
            op.dtype,
        )

    def visit_ExtractEpochSeconds(self, op, *, arg):
        return self.f.unix_timestamp(self.cast(arg, dt.string))

    def visit_Cast(self, op, *, arg, to):
        from_ = op.arg.dtype
        if to.is_timestamp():
            if from_.is_numeric():
                if not from_.is_integer():
                    raise com.UnsupportedOperationError(
                        "Flink does not support casting non-integer numbers to timestamp"
                    )
                arg = self.f.from_unixtime(arg)
            if (tz := to.timezone) is not None:
                return self.f.to_timestamp(
                    self.f.convert_tz(self.cast(arg, dt.string), "UTC+0", tz)
                )
            else:
                from ibis.common.temporal import TimestampUnit

                if to.unit == TimestampUnit.SECOND:
                    return self.f.to_timestamp(arg)
                return self.f.to_timestamp(arg, "yyyy-MM-dd HH:mm:ss.SSS")
        elif to.is_json():
            return arg
        elif from_.is_temporal() and to.is_int64():
            return 1_000_000 * self.f.unix_timestamp(arg)
        else:
            return self.cast(arg, to)

    def visit_IfElse(self, op, *, bool_expr, true_expr, false_null_expr):
        return self.if_(
            bool_expr,
            true_expr if true_expr != NULL else self.cast(true_expr, op.dtype),
            (
                false_null_expr
                if false_null_expr != NULL
                else self.cast(false_null_expr, op.dtype)
            ),
        )

    def visit_Log10(self, op, *, arg):
        return self.f.anon.log(10, arg)

    def visit_ExtractMillisecond(self, op, *, arg):
        return self.f.extract(self.v.millisecond, arg)

    def visit_ExtractMicrosecond(self, op, *, arg):
        return self.f.extract(self.v.microsecond, arg)

    def visit_DayOfWeekIndex(self, op, *, arg):
        return (self.f.anon.dayofweek(arg) + 5) % 7

    def visit_DayOfWeekName(self, op, *, arg):
        index = self.cast(self.f.anon.dayofweek(self.cast(arg, dt.date)), op.dtype)
        lookup_table = self.f.anon.str_to_map(
            "1=Sunday,2=Monday,3=Tuesday,4=Wednesday,5=Thursday,6=Friday,7=Saturday"
        )
        return lookup_table[index]

    def visit_TimestampNow(self, op):
        return self.v.current_timestamp

    def visit_DateNow(self, op):
        return self.v.current_date

    def visit_TimestampBucket(self, op, *, arg, interval, offset):
        unit = op.interval.dtype.unit.name
        unit_var = self.v[unit]

        if offset is None:
            offset = 0
        else:
            offset = op.offset.value

        bucket_width = op.interval.value
        unit_func = self.f["dayofmonth" if unit.upper() == "DAY" else unit]

        arg = self.f.anon.timestampadd(unit_var, -sge.paren(offset, copy=False), arg)
        mod = unit_func(arg) % bucket_width

        return self.f.anon.timestampadd(
            unit_var,
            -sge.paren(mod, copy=False) + offset,
            self.v[f"FLOOR({arg.sql(self.dialect)} TO {unit_var.sql(self.dialect)})"],
        )

    def visit_TemporalDelta(self, op, *, part, left, right):
        right = self.visit_TemporalTruncate(None, arg=right, unit=part)
        left = self.visit_TemporalTruncate(None, arg=left, unit=part)
        return self.f.anon.timestampdiff(
            self.v[part.this],
            self.cast(right, dt.timestamp),
            self.cast(left, dt.timestamp),
        )

    visit_TimeDelta = visit_DateDelta = visit_TimestampDelta = visit_TemporalDelta

    def visit_TemporalTruncate(self, op, *, arg, unit):
        unit_var = self.v[unit.name]
        arg_sql = arg.sql(self.dialect)
        unit_sql = unit_var.sql(self.dialect)
        return self.f.floor(self.v[f"{arg_sql} TO {unit_sql}"])

    visit_TimestampTruncate = visit_DateTruncate = visit_TimeTruncate = (
        visit_TemporalTruncate
    )

    def visit_StringContains(self, op, *, haystack, needle):
        return self.f.instr(haystack, needle) > 0

    def visit_StringFind(self, op, *, arg, substr, start, end):
        if end is not None:
            raise com.UnsupportedOperationError(
                "String find doesn't support `end` argument"
            )

        if start is not None:
            arg = self.f.substr(arg, start + 1)
            pos = self.f.instr(arg, substr)
            return self.if_(pos > 0, pos + start, 0)

        return self.f.instr(arg, substr)

    def visit_LPad(self, op, *, arg, length, pad):
        return self.if_(
            length <= self.f.length(arg),
            arg,
            self.f.concat(self.f.repeat(pad, length - self.f.length(arg)), arg),
        )

    def visit_RPad(self, op, *, arg, length, pad):
        return self.if_(
            length <= self.f.length(arg),
            arg,
            self.f.concat(arg, self.f.repeat(pad, length - self.f.length(arg))),
        )

    def visit_StartsWith(self, op, *, arg, start):
        return self.f.left(arg, self.f.char_length(start)).eq(start)

    def visit_EndsWith(self, op, *, arg, end):
        return self.f.right(arg, self.f.char_length(end)).eq(end)

    def visit_ExtractUrlField(self, op, *, arg):
        return self.f.parse_url(arg, type(op).__name__[len("Extract") :].upper())

    visit_ExtractAuthority = visit_ExtractHost = visit_ExtractUserInfo = (
        visit_ExtractProtocol
    ) = visit_ExtractFile = visit_ExtractPath = visit_ExtractUrlField

    def visit_ExtractQuery(self, op, *, arg, key):
        return self.f.parse_url(*filter(None, (arg, "QUERY", key)))

    def visit_ExtractFragment(self, op, *, arg):
        return self.f.parse_url(arg, "REF")

    def visit_CountStar(self, op, *, arg, where):
        if where is None:
            return self.f.count(STAR)
        return self.f.sum(self.cast(where, dt.int64))

    def visit_CountDistinct(self, op, *, arg, where):
        if where is not None:
            arg = self.if_(where, arg, self.f.array(arg)[2])
        return self.f.count(sge.Distinct(expressions=[arg]))

    def visit_MapContains(self, op: ops.MapContains, *, arg, key):
        key_type = op.arg.dtype.key_type
        return self.f.array_contains(
            self.cast(self.f.map_keys(arg), dt.Array(value_type=key_type)),
            self.cast(key, key_type),
        )

    def visit_Map(self, op: ops.Map, *, keys, values):
        return self.cast(self.f.map_from_arrays(keys, values), op.dtype)

    def visit_MapMerge(self, op: ops.MapMerge, *, left, right):
        left_keys = self.f.map_keys(left)
        left_values = self.f.map_values(left)

        right_keys = self.f.map_keys(right)
        right_values = self.f.map_values(right)

        keys = self.f.array_concat(left_keys, right_keys)
        values = self.f.array_concat(left_values, right_values)

        return self.cast(self.f.map_from_arrays(keys, values), op.dtype)

    def visit_StructColumn(self, op, *, names, values):
        return self.cast(sge.Struct(expressions=list(values)), op.dtype)

    def visit_ArrayCollect(self, op, *, arg, where, order_by, include_null, distinct):
        if order_by:
            raise com.UnsupportedOperationError(
                "ordering of order-sensitive aggregations via `order_by` is "
                "not supported for this backend"
            )
        if not include_null:
            cond = arg.is_(sg.not_(NULL, copy=False))
            where = cond if where is None else sge.And(this=cond, expression=where)
        out = self.f.array_agg(arg)
        if where is not None:
            out = sge.Filter(this=out, expression=sge.Where(this=where))
        if distinct:
            # TODO: Flink supposedly supports `ARRAY_AGG(DISTINCT ...)`, but it
            # doesn't work with filtering (either `include_null=False` or
            # additional filtering). Their `array_distinct` function does maintain
            # ordering though, so we can use it here.
            out = self.f.array_distinct(out)
        return out

    def visit_Strip(self, op, *, arg):
        # TODO: at some point, the upstream `BTRIM` function should work, but it
        # currently doesn't, so we use a combination of left and right trim here
        return self.visit_RStrip(op, arg=self.visit_LStrip(op, arg=arg))


compiler = FlinkCompiler()
