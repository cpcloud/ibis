import ibis.expr.operations as ops
import ibis.expr.types as ir
import ibis.expr.datatypes as dt


class PandasClient(object):

    def __init__(self, dictionary):
        self.dictionary = dictionary

    def table(self, name):
        return ops.DatabaseTable(name, schema, self.dictionary[name])

    def execute(self, query, *args, **kwargs):
        assert isinstance(query, ir.Expr)
        return execute(query)


class PandasDatabase(ops.Database):

    def __init__(self, client, name=None):
        super().__init__(client)

    def list_tables(self):
        return sorted(self.client.keys())

    def table(self, name):
        return self.client[name]

    def drop(self):
        self.client.clear()


