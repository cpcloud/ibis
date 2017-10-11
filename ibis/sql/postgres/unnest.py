from sqlalchemy.sql import functions
from sqlalchemy.sql.selectable import FromClause, Alias
from sqlalchemy.sql.elements import ColumnClause
from sqlalchemy.ext.compiler import compiles


class FunctionColumn(ColumnClause):
    def __init__(self, function, name, type_=None):
        self.function = self.table = function
        self.name = self.key = name
        self.type = type_
        self.is_literal = False

    @property
    def _from_objects(self):
        return []

    def _make_proxy(self, selectable, name=None, attach=True,
                    name_is_truncatable=False, **kw):
        if self.name == self.function.name:
            name = selectable.name
        else:
            name = self.name

        co = ColumnClause(name, self.type)
        co.key = self.name
        co._proxies = [self]
        if selectable._is_clone_of is not None:
            co._is_clone_of = \
                selectable._is_clone_of.columns.get(co.key)
        co.table = selectable
        co.named_with_table = True
        if attach:
            selectable._columns[co.key] = co
        return co


@compiles(FunctionColumn)
def _compile_function_column(element, compiler, **kw):
    if kw.get('asfrom', False):
        return '({}).{}'.format(
            compiler.process(element.function, **kw),
            compiler.preparer.quote(element.name)
        )
    return element.name


class PGAlias(Alias):
    pass


@compiles(PGAlias)
def _compile_pg_alias(element, compiler, **kw):
    text = compiler.visit_alias(element, **kw)
    if kw['asfrom']:
        text += '({})'.format(', '.join(col.name for col in element.element.c))
    return text


class ColumnFunction(functions.FunctionElement):
    __visit_name__ = 'function'

    @property
    def columns(self):
        return FromClause.columns.fget(self)

    def _populate_column_collection(self):
        for name in self.column_names:
            self._columns[name] = FunctionColumn(self, name)

    def alias(self, name):
        return PGAlias(self, name)


def unnest(obj):
    function = type(
        'unnest',
        (ColumnFunction,),
        {'name': 'unnest', 'column_names': [getattr(obj, 'element', obj).name]}
    )

    return function(obj)
