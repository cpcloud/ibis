from __future__ import annotations

from abc import abstractmethod

from matchpy import Arity, Operation
from public import public

import ibis.expr.rules as rlz
from ibis.common.exceptions import ExpressionError
from ibis.common.grounds import Annotable, Comparable
from ibis.expr.rules import Shape
from ibis.expr.schema import Schema
from ibis.util import UnnamedMarker, is_iterable


@public
class Node(Annotable, Comparable):
    def __init_subclass__(
        cls,
        /,
        name=None,
        arity=False,
        associative=False,
        commutative=False,
        one_identity=False,
        infix=False,
        **kwargs,
    ):
        cls.__pattern__ = Operation.new(
            head=cls,
            name=name or cls.__name__,
            arity=arity or Arity(len(cls.argnames), True),
            associative=associative,
            commutative=commutative,
            one_identity=one_identity,
            infix=infix,
        )

    @classmethod
    def pattern(cls, *args, **kwargs):
        params = tuple(v for _, v in cls.__signature__.apply(*args, **kwargs))
        return cls.__pattern__(*params)

    def __len__(self):
        return len(self.args)

    def __iter__(self):
        return iter(self.args)

    def __equals__(self, other):
        return self.args == other.args

    def equals(self, other):
        if not isinstance(other, Node):
            raise TypeError(
                "invalid equality comparison between Node and "
                f"{type(other)}"
            )
        return self.__cached_equals__(other)

    # TODO(kszucs): remove this property
    @property
    def inputs(self):
        return self.args

    @abstractmethod
    def to_expr(self):
        ...

    # TODO(kszucs): introduce a HasName schema, or NamedValue with a .name
    # abstractproperty
    def resolve_name(self):
        raise ExpressionError(f'Expression is not named: {type(self)}')

    def has_resolved_name(self):
        return False

    # TODO(kszucs): remove this method entirely
    def flat_args(self):
        for arg in self.args:
            if not isinstance(arg, (Schema, Node)) and is_iterable(arg):
                yield from arg
            else:
                yield arg


Operation.register(Node)


@public
class Value(Node):
    @property
    @abstractmethod
    def output_dtype(self):
        """
        Ibis datatype of the produced value expression.

        Returns
        -------
        dt.DataType
        """

    @property
    @abstractmethod
    def output_shape(self):
        """
        Shape of the produced value expression.

        Possible values are: "scalar" and "columnar"

        Returns
        -------
        rlz.Shape
        """

    def to_expr(self):
        if self.output_shape is Shape.COLUMNAR:
            return self.output_dtype.column(self)
        else:
            return self.output_dtype.scalar(self)


@public
class Alias(Value):
    arg = rlz.any
    name = rlz.instance_of((str, UnnamedMarker))

    output_shape = rlz.shape_like("arg")
    output_dtype = rlz.dtype_like("arg")

    def has_resolved_name(self):
        return True

    def resolve_name(self):
        return self.name

    def __iter__(self):
        return iter(())


@public
class Unary(Value):
    """A unary operation."""

    arg = rlz.any

    @property
    def output_shape(self):
        return self.arg.output_shape


@public
class Binary(Value):
    """A binary operation."""

    left = rlz.any
    right = rlz.any

    @property
    def output_shape(self):
        return max(self.left.output_shape, self.right.output_shape)


public(ValueOp=Value, UnaryOp=Unary, BinaryOp=Binary)
