from __future__ import annotations

import itertools
from abc import ABCMeta, abstractmethod
from copy import copy
from typing import Any, Mapping, Sequence
from weakref import WeakValueDictionary

import matchpy

from ibis.common.annotations import Argument, Attribute, Signature
from ibis.common.caching import WeakCache
from ibis.common.graph import Graph, Traversable
from ibis.common.typing import evaluate_typehint
from ibis.common.validators import Validator
from ibis.util import frozendict, recursive_get, recursive_iter


class BaseMeta(ABCMeta):

    __slots__ = ()

    def __new__(metacls, clsname, bases, dct, **kwargs):
        # enforce slot definitions
        dct.setdefault("__slots__", ())
        return super().__new__(metacls, clsname, bases, dct, **kwargs)

    def __call__(cls, *args, **kwargs) -> Base:
        return cls.__create__(*args, **kwargs)


class Base(metaclass=BaseMeta):

    __slots__ = ('__weakref__',)

    @classmethod
    def __create__(cls, *args, **kwargs) -> Base:
        return type.__call__(cls, *args, **kwargs)


class AnnotableMeta(BaseMeta):
    """Metaclass to turn class annotations into a validatable function
    signature."""

    __slots__ = ()

    def __new__(metacls, clsname, bases, dct, **kwargs):
        # inherit signature from parent classes
        signatures, attributes = [], {}
        for parent in bases:
            try:
                attributes.update(parent.__attributes__)
            except AttributeError:
                pass
            try:
                signatures.append(parent.__signature__)
            except AttributeError:
                pass

        # collection type annotations and convert them to validators
        module = dct.get('__module__')
        annots = dct.get('__annotations__', {})
        for name, annot in annots.items():
            typehint = evaluate_typehint(annot, module)
            validator = Validator.from_annotation(typehint)
            if name in dct:
                dct[name] = Argument.default(dct[name], validator)
            else:
                dct[name] = Argument.mandatory(validator)

        # collect the newly defined annotations
        slots = list(dct.pop('__slots__', []))
        namespace, arguments = {}, {}
        for name, attrib in dct.items():
            if isinstance(attrib, Validator):
                attrib = Argument.mandatory(attrib)

            if isinstance(attrib, Argument):
                arguments[name] = attrib
                attributes[name] = attrib
                slots.append(name)
            elif isinstance(attrib, Attribute):
                attributes[name] = attrib
                slots.append(name)
            else:
                namespace[name] = attrib

        # merge the annotations with the parent annotations
        signature = Signature.merge(*signatures, **arguments)
        argnames = tuple(signature.parameters.keys())

        namespace.update(
            __argnames__=argnames,
            __attributes__=attributes,
            __match_args__=argnames,
            __signature__=signature,
            __slots__=tuple(slots),
        )
        return super().__new__(metacls, clsname, bases, namespace, **kwargs)


class Immutable(Base):
    def __setattr__(self, name: str, _: Any) -> None:
        raise TypeError(
            f"Attribute {name!r} cannot be assigned to immutable instance of "
            f"type {type(self)}"
        )


class Singleton(Base):
    __instances__ = WeakValueDictionary()

    @classmethod
    def __create__(cls, *args, **kwargs) -> Singleton:
        key = (cls, args, frozendict(kwargs))
        try:
            return cls.__instances__[key]
        except KeyError:
            instance = super().__create__(*args, **kwargs)
            cls.__instances__[key] = instance
            return instance


class Annotable(Base, metaclass=AnnotableMeta):
    """Base class for objects with custom validation rules."""

    @classmethod
    def __create__(cls, *args, **kwargs):
        kwargs.pop("variable_name", None)

        if any(
            isinstance(arg, matchpy.Expression)
            for arg in itertools.chain(
                args,
                [v for v in kwargs.values() if not isinstance(v, tuple)],
                *[v for v in kwargs.values() if isinstance(v, tuple)],
            )
        ):
            return cls.pattern(*args, **kwargs)

        # construct the instance by passing the validated keyword arguments
        kwargs = cls.__signature__.validate(*args, **kwargs)
        return super().__create__(**kwargs)

    def __init__(self, **kwargs) -> None:
        # set the already validated fields using object.__setattr__
        for name, value in kwargs.items():
            object.__setattr__(self, name, value)
        # allow child classes to do some post-initialization
        self.__post_init__()

    def __post_init__(self) -> None:
        for name, field in self.__attributes__.items():
            if isinstance(field, Attribute):
                value = field.initialize(self)
                if value is not None:
                    object.__setattr__(self, name, value)

    def __setattr__(self, name, value) -> None:
        if field := self.__attributes__.get(name):
            value = field.validate(value, this=self)
        super().__setattr__(name, value)

    def __repr__(self) -> str:
        args = (f"{n}={getattr(self, n)!r}" for n in self.__argnames__)
        argstring = ", ".join(args)
        return f"{self.__class__.__name__}({argstring})"

    def __eq__(self, other) -> bool:
        if type(self) is not type(other):
            return NotImplemented

        return all(
            getattr(self, n, None) == getattr(other, n, None)
            for n in self.__attributes__.keys()
        )

    def __getstate__(self) -> dict[str, Any]:
        return {n: getattr(self, n, None) for n in self.__attributes__.keys()}

    def __setstate__(self, state) -> None:
        self.__init__(**state)

    def copy(self, **overrides: Any) -> Annotable:
        """Return a copy of this object with the given overrides.

        Parameters
        ----------
        overrides
            Argument override values

        Returns
        -------
        Annotable
            New instance of the copied object
        """
        this = copy(self)
        for name, value in overrides.items():
            if field := self.__attributes__.get(name):
                value = field.validate(value, this=this)
            object.__setattr__(this, name, value)
        this.__post_init__()
        return this


class _Operation(matchpy.Operation):
    def __lshift__(self, other):
        if isinstance(other, Matchable):
            pattern = matchpy.Pattern(self)
            return next(matchpy.match(other, pattern))
        else:
            return matchpy.substitute(self, other)

    __rrshift__ = __lshift__


class Matchable(Base):

    __slots__ = ()

    def __init_subclass__(
        cls,
        /,
        name=None,
        arity=False,
        associative=False,
        commutative=False,
        one_identity=False,
        infix=False,
        unpacked_args_to_init=False,
        **kwargs,
    ):
        # support python 3.10 pattern matching
        cls.__match_args__ = cls.__argnames__
        # integrate with matchpy by creating a matchpy specific operation
        cls.__pattern__ = _Operation.new(
            head=cls,
            name=name or cls.__name__,
            arity=arity or matchpy.Arity(len(cls.__argnames__), True),
            associative=associative,
            commutative=commutative,
            one_identity=one_identity,
            infix=infix,
        )
        cls.__pattern__.unpacked_args_to_init = unpacked_args_to_init
        # need to register the current class as a subclass of matchpy's
        # Operation class, so that the operation class instandiated above
        # can be matched agains an actual instance of the current class

    @classmethod
    def pattern(cls, *args, **kwargs):
        params = cls.__signature__.apply(*args, **kwargs)
        return cls.__pattern__(*params.values())

    def __len__(self):
        return len(self.__args__)

    def __getitem__(self, key):
        return self.__args__[key]

    def __rshift__(self, args):
        if isinstance(args, _Operation):
            operation = args
        elif isinstance(args, Sequence):
            operation = self.pattern(*args)
        elif isinstance(args, Mapping):
            operation = self.pattern(**args)
        else:
            raise TypeError(f"Unable to construct a matchable pattern from `{args}`")
        pattern = matchpy.Pattern(operation)
        return next(matchpy.match(self, pattern))

    __rlshift__ = __rshift__


# need to register the current class as a subclass of matchpy's Operation class
# so that the operation class instandiated above can be matched agains an
# actual instance of the current class
matchpy.Operation.register(Matchable)


class Comparable(Base):

    __cache__ = WeakCache()

    def __eq__(self, other) -> bool:
        try:
            return self.__cached_equals__(other)
        except TypeError:
            return NotImplemented  # noqa: F901

    @abstractmethod
    def __equals__(self, other) -> bool:
        ...

    def __cached_equals__(self, other) -> bool:
        if self is other:
            return True

        # type comparison should be cheap
        if type(self) is not type(other):
            return False

        # reduce space required for commutative operation
        if id(self) < id(other):
            key = (self, other)
        else:
            key = (other, self)

        try:
            result = self.__cache__[key]
        except KeyError:
            result = self.__equals__(other)
            self.__cache__[key] = result

        return result


class Concrete(Immutable, Comparable, Annotable, Traversable, Matchable):
    """Opinionated base class for immutable data classes."""

    __slots__ = ("__args__", "__children__", "__precomputed_hash__")

    def __post_init__(self) -> None:
        # optimizations to store frequently accessed attributes
        args = tuple(getattr(self, name) for name in self.__argnames__)
        # precompute children for faster traversal
        children = (c for c in recursive_iter(args) if isinstance(c, Concrete))
        # precompute the hash value to avoid repeating expensive hashing
        hashvalue = hash((self.__class__, args))
        # actually set the attributes
        object.__setattr__(self, "__args__", args)
        object.__setattr__(self, "__children__", tuple(children))
        object.__setattr__(self, "__precomputed_hash__", hashvalue)
        # initialize the remaining attributes
        super().__post_init__()

    def __getstate__(self):
        # assuming immutability and idempotency of the __init__ method, we can
        # reconstruct the instance from the arguments
        return dict(zip(self.__argnames__, self.__args__))

    def __hash__(self):
        return self.__precomputed_hash__

    def __equals__(self, other):
        return self.__args__ == other.__args__

    @property
    def args(self):
        return self.__args__

    @property
    def argnames(self):
        return self.__argnames__

    def map(self, fn, filter=None):
        if filter is None:
            filter = Concrete

        results = {}
        for node in Graph.from_bfs(self, filter=filter).toposort():
            args, kwargs = node.__signature__.unbind(node)
            args = recursive_get(args, results)
            kwargs = recursive_get(kwargs, results)
            results[node] = fn(node, *args, **kwargs)

        return results

    def substitute(self, fn, filter=None):
        return self.map(fn, filter=filter)[self]

    def replace(self, subs, filter=None):
        def fn(node, *args, **kwargs):
            try:
                return subs[node]
            except KeyError:
                return node.__class__(*args, **kwargs)

        return self.substitute(fn, filter=filter)
