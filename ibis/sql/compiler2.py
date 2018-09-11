"""Version 2 of the ibis SQL compiler

Dependencies:

* Hashable expressions #DONE
* An "unsafe" expression substitution function that will recursively replace
  all the things  #DONE
* Remove the notion of a "blocking" operation.

#. Expression comes in as unoptimized

#. Optimize the expression (no-op right now, these are potentially interesting
   to implement)
   * Predicate fusion (collapse predicates across subqueries)
   * Projection fusion (collapse projections across subqueries)
   * Push down projections (change all `*` selections into column references if
                            possible)
   * Constant propagation (replace all references to literals with the literal)
   * Constant folding (evaluate any constants)
   * Extract subqueries (find all subqueries used more than once ->
                         form a CTE -> replace all subqueries with the CTE)

#. Convert to a simpler tree consisting of select, filter, column
   #. At this point our expression has been type checked

#. Provide aliases to unnamed subqueries
#. The query is compiled into a string
"""

import collections

import toolz

from multipledispatch import Dispatcher

import ibis.expr.types as ir

from ibis.common import IbisTypeError


transform = Dispatcher('transform')


@transform.register(ir.Expr, collections.Mapping)
def transform_expr(expr, mapping, recur=None):
    return recur(expr, mapping)


@transform.register(object, collections.Mapping)
def transform_other(obj, mapping, substitutor=None):
    return obj


def substitute(expr, mapping):
    @toolz.memoize(key=lambda args, kwargs: args[0]._key)
    def substitutor(expr, mapping):
        node = expr.op()
        remapping = {old.op(): new for old, new in mapping.items()}
        result = remapping.get(node)
        if result is not None:
            return result
        else:
            node_type = type(node)
            try:
                new_node = node_type(
                    *(transform(arg, mapping, recur=substitutor)
                        for arg in node.args)
                )
            except IbisTypeError:
                return expr
            else:
                return expr._factory(new_node, name=expr._safe_name)
    return substitutor(expr, mapping)


class Optimizer:
    pass


class Optimization:
    pass


class ReassociateConstants(Optimization):
    pass


class ConstantFold(Optimization):
    pass
