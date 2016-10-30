# Copyright 2014 Cloudera Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import sqlalchemy as sa

from operator import methodcaller, le, ge
from functools import partial
from toolz import identity

from ibis.sql.alchemy import unary, varargs, fixed_arity
from ibis.sql.adapt import adapt
from ibis.expr.lineage import traverse
from ibis.expr.analysis import sub_for
import ibis.sql.alchemy as alch
import ibis.expr.datatypes as dt
import ibis.expr.operations as ops
import ibis.expr.types as ir
import ibis.common as com


_operation_registry = alch._operation_registry.copy()


def _cast(t, expr):
    # It's not all fun and games with SQLite

    op = expr.op()
    arg, target_type = op.args
    sa_arg = t.translate(arg)
    sa_type = t.get_sqla_type(target_type)

    # SQLite does not have a physical date/time/timestamp type, so
    # unfortunately cast to typestamp must be a no-op, and we have to trust
    # that the user's data can actually be correctly parsed by SQLite.
    if isinstance(target_type, dt.Timestamp):
        if not isinstance(arg, (ir.IntegerValue, ir.StringValue)):
            raise com.TranslationError(type(arg))

        return sa_arg

    if isinstance(arg, ir.CategoryValue) and target_type == 'int32':
        return sa_arg
    else:
        return sa.cast(sa_arg, sa_type)


def _substr(t, expr):
    f = sa.func.substr

    arg, start, length = expr.op().args

    sa_arg = t.translate(arg)
    sa_start = t.translate(start)

    if length is None:
        return f(sa_arg, sa_start + 1)
    else:
        sa_length = t.translate(length)
        return f(sa_arg, sa_start + 1, sa_length)


def _string_right(t, expr):
    f = sa.func.substr

    arg, length = expr.op().args

    sa_arg = t.translate(arg)
    sa_length = t.translate(length)

    return f(sa_arg, -sa_length, sa_length)


def _string_find(t, expr):
    arg, substr, start, _ = expr.op().args

    if start is not None:
        raise NotImplementedError

    sa_arg = t.translate(arg)
    sa_substr = t.translate(substr)

    f = sa.func.instr
    return f(sa_arg, sa_substr) - 1


def _infix_op(infix_sym):
    def formatter(t, expr):
        op = expr.op()
        left, right = op.args

        left_arg = t.translate(left)
        right_arg = t.translate(right)
        return left_arg.op(infix_sym)(right_arg)

    return formatter


def _strftime(t, expr):
    arg, format = expr.op().args
    sa_arg = t.translate(arg)
    sa_format = t.translate(format)
    return sa.func.strftime(sa_format, sa_arg)


def _strftime_int(fmt):
    def translator(t, expr):
        arg, = expr.op().args
        sa_arg = t.translate(arg)
        return sa.cast(sa.func.strftime(fmt, sa_arg), sa.types.INTEGER)
    return translator


def _now(t, expr):
    return sa.func.datetime('now')


def _millisecond(t, expr):
    arg, = expr.op().args
    sa_arg = t.translate(arg)
    fractional_second = sa.func.strftime('%f', sa_arg)
    return (fractional_second * 1000) % 1000


_operation_registry.update({
    ops.Cast: _cast,

    ops.Substring: _substr,
    ops.StrRight: _string_right,

    ops.StringFind: _string_find,

    ops.StringLength: unary('length'),

    ops.Least: varargs(sa.func.min),
    ops.Greatest: varargs(sa.func.max),
    ops.IfNull: fixed_arity(sa.func.ifnull, 2),

    ops.Lowercase: unary('lower'),
    ops.Uppercase: unary('upper'),

    ops.Strip: unary('trim'),
    ops.LStrip: unary('ltrim'),
    ops.RStrip: unary('rtrim'),

    ops.StringReplace: fixed_arity(sa.func.replace, 3),
    ops.StringSQLLike: _infix_op('LIKE'),
    ops.RegexSearch: _infix_op('REGEXP'),

    ops.Strftime: _strftime,
    ops.ExtractYear: _strftime_int('%Y'),
    ops.ExtractMonth: _strftime_int('%m'),
    ops.ExtractDay: _strftime_int('%d'),
    ops.ExtractHour: _strftime_int('%H'),
    ops.ExtractMinute: _strftime_int('%M'),
    ops.ExtractSecond: _strftime_int('%S'),
    ops.ExtractMillisecond: _millisecond,
    ops.TimestampNow: _now
})


def add_operation(op, translation_func):
    _operation_registry[op] = translation_func


class SQLiteExprTranslator(alch.AlchemyExprTranslator):

    _registry = _operation_registry
    _rewrites = alch.AlchemyExprTranslator._rewrites.copy()
    _type_map = alch.AlchemyExprTranslator._type_map.copy()
    _type_map.update({
        dt.Double: sa.types.REAL,
        dt.Float: sa.types.REAL
    })


rewrites = SQLiteExprTranslator.rewrites
compiles = SQLiteExprTranslator.compiles


class SQLiteDialect(alch.AlchemyDialect):

    translator = SQLiteExprTranslator



@adapt.register(ir.ArrayExpr, SQLiteDialect)
def adapt_array_expr_sqlite(expr, dialect):
    result, handler = adapt(expr, None)
    windows = list(traverse(result, node_types=ops.WindowOp))
    if not windows:
        return result
    else:
        new_windows = list(map(rewrite_window_as_projection, windows))
        source1, agg1, joiner1, oldagg1 = new_windows[0]

        joined = joiner1(source1)
        new_proj = [source1, agg1]
        oldaggs = [oldagg1]
        for source, agg, joiner, oldagg in new_windows[1:]:
            assert source1.equals(source)
            joined = joiner(joined)
            new_proj.append(agg)
            oldaggs.append(oldagg)

    new_base_relation = joined.projection(new_proj)
    subbed_result = sub_for(
        expr,
        [
            (source1, new_base_relation),
        ] + [
            (oldagg, new_base_relation[oldagg._name]) for oldagg in oldaggs
        ]
    )

    projected = new_base_relation[subbed_result.name(expr._name or 'tmp')]
    return projected, identity


def rewrite_window_as_projection(window_expr):
    """
    """
    window_op = window_expr.op()
    agg, window = window_op.args

    agg_op = agg.op()
    agg_type = type(agg_op)
    column, _ = agg_op.args

    source_relation = column._arg.table

    group_by_names = [e._name for e in window._group_by]
    order_by_names = [
        op.expr._name for op in map(methodcaller('op'), window._order_by)
    ]

    all_names = group_by_names + order_by_names

    ORDERING_OPS = {True: le, False: ge}

    if window._order_by:
        # if we have an order by we need to do a self join, then do our partitioning
        left, right = source_relation, source_relation.view()
        left = left.projection([left[o].name(o) for o in all_names]).distinct()
        ordering_source = left.inner_join(
            right,
            [
                ORDERING_OPS[ascending](right[name], left[name])
                for name, ascending in (
                    (op.expr._name, op.ascending)
                    for op in map(methodcaller('op'), window._order_by)
                )
            ] + [
                left[name] == right[name] for name in group_by_names
            ]
        )
        projection = [
            left[name] for name in order_by_names
        ] + [
            right[name] for name in group_by_names
        ]
        if column._name not in order_by_names:
            projection.append(right[column._name])
        ordering_source = ordering_source.projection(projection)
    else:
        ordering_source = source_relation

    new_agg_source = ordering_source.group_by(all_names).aggregate(
        agg_type(ordering_source[column._name]).to_expr().name(agg._name)
    )

    if not group_by_names and not order_by_names:
        join_type = 'cross'
    else:
        join_type = 'left'

    predicates = [
        source_relation[name] == new_agg_source[name]
        for name in all_names
    ]

    joiner = partial(
        lambda predicates, other_relation, relation: getattr(
            relation, '{}_join'.format(join_type)
        )(other_relation, predicates=predicates),
        predicates,
        new_agg_source,
    )

    return (
        source_relation,  # the final relation that we join with
        new_agg_source[agg._name].name(
            window_expr._name
        ),  # the aggregated column
        joiner,  # join composition function
        agg,  # old agg to replace
    )
