import sly

import ibis.expr.datatypes as dt
from ibis.expr.lexer import lexer


class DataTypeParser(sly.Parser):
    tokens = lexer.tokens

    @_('primitive',
       'timestamp',
       'interval',
       'decimal',
       'array',
       'map',
       'struct')
    def type(self, p):
        return p[0]

    @_('ANY', 'NULL', 'PRIMITIVE', 'TIME')
    def primitive(self, p):
        return dict(dt._primitive_types)[p[0].lower()]

    @_('TIMESTAMP LPAREN STRARG RPAREN')
    def timestamp(self, p):
        return dt.Timestamp(p.STRARG)

    @_('TIMESTAMP')
    def timestamp(self, p):
        return dt.timestamp

    @_('INTERVAL LBRACKET type RBRACKET LPAREN UNIT RPAREN')
    def interval(self, p):
        return dt.Interval(unit=p.UNIT, value_type=p.type)

    @_('INTERVAL LPAREN UNIT RPAREN')
    def interval(self, p):
        return dt.Interval(unit=p.UNIT)

    @_('INTERVAL')
    def interval(self, p):
        return dt.interval

    @_('DECIMAL LPAREN INTEGER COMMA INTEGER RPAREN')
    def decimal(self, p):
        return dt.Decimal(int(p.INTEGER0), int(p.INTEGER1))

    @_('DECIMAL')
    def decimal(self, p):
        return dt.decimal

    @_('ARRAY LBRACKET type RBRACKET')
    def array(self, p):
        return dt.Array(p.type)

    @_('MAP LBRACKET primitive COMMA type RBRACKET')
    def map(self, p):
        return dt.Map(p.primitive, p.type)

    @_('MAP LBRACKET error COMMA type RBRACKET')
    def map(self, p):
        raise SyntaxError('Invalid map key type {!r}'.format(p.error.value))

    @_('STRUCT LBRACKET struct_fields RBRACKET')
    def struct(self, p):
        return dt.Struct.from_tuples(p.struct_fields)

    @_('struct_fields COMMA struct_field')
    def struct_fields(self, p):
        p.struct_fields.append(p.struct_field)
        return p.struct_fields

    @_('struct_field')
    def struct_fields(self, p):
        return [p.struct_field]

    @_('FIELD COLON type')
    def struct_field(self, p):
        return p.FIELD, p.type


parser = DataTypeParser()


def parse(typestring):
    return parser.parse(lexer.tokenize(typestring))


print(parse('array<struct<foo: int32, baz: struct<foo: array<map<string, array<double>>>>, bar: array<string>>>'))
