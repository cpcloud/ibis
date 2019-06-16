"""Ibis's custom exceptions."""


class HDFSError(Exception):
    """Raised for exceptional conditions related to HDFS."""


class DependencyMissingError(Exception):
    """Raised when a dependency is missing."""


class InternalError(Exception):
    """Raised on internal exceptional conditions."""


class IntegrityError(Exception):
    """Raised for integrity errors."""


class ExpressionError(Exception):
    """Raised for generic expression errors."""


class InvalidRelationError(Exception):
    """Raised when an invalid relation is encountered."""


class TranslationError(Exception):
    """Base class for exception related to expression translation."""


class OperationNotDefinedError(TranslationError):
    """Raised when an operation is not defined."""


class UnsupportedOperationError(TranslationError):
    """Raised when an operation is unsupported by a particular backend."""


class UnsupportedBackendType(TranslationError):
    """Raised when a data type is unsupported by a particular backend."""


class UnboundExpressionError(Exception):
    """Raised when an expression is not bound to any data."""


class IbisInputError(Exception):
    """Raised when the value of an input is invalid."""


class IbisTypeError(Exception):
    """Raised for generic type errors specific to ibis."""


class IbisInputTypeError(IbisInputError, IbisTypeError):
    """Raised for type errors specific to ibis operation inputs."""


class InvalidArgumentError(Exception):
    """Raised when an invalid argument is passed to a callable."""


class UnsupportedArgumentError(Exception):
    """Raised when an unsupported argument is encountered."""
