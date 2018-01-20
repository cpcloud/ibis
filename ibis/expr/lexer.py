from pprint import pprint

import ibis.expr.datatypes as dt

import sly

_STRING_REGEX = """('[^\n'\\\\]*(?:\\\\.[^\n'\\\\]*)*'|"[^\n"\\\\"]*(?:\\\\.[^\n"\\\\]*)*")"""  # noqa: E501


class DataTypeLexer(sly.Lexer):
    tokens = {
        'ANY',
        'NULL',
        'PRIMITIVE',
        'TIMESTAMP',
        'INTERVAL',
        'TIME',
        'DECIMAL',
        'VARCHAR',
        'CHAR',
        'ARRAY',
        'MAP',
        'STRUCT',
        'INTEGER',
        'FIELD',
        'COMMA',
        'COLON',
        'LPAREN',
        'RPAREN',
        'LBRACKET',
        'RBRACKET',
        'STRARG',
        'UNIT',
    }

    ignore = ' \t\n'

    ANY = 'any|ANY'
    NULL = 'null|NULL'
    PRIMITIVE = '|'.join(
        '({}|{})'.format(k.lower(), k.upper()) for k, _ in dt._primitive_types
    )
    TIMESTAMP = 'timestamp|TIMESTAMP'
    INTERVAL = 'interval|INTERVAL'
    TIME = 'time|TIME'
    DECIMAL = 'decimal|DECIMAL'
    VARCHAR = 'varchar|VARCHAR'
    CHAR = 'char|CHAR'
    ARRAY = 'array|ARRAY'
    MAP = 'map|MAP'
    STRUCT = 'struct|STRUCT'

    INTEGER = r'\d+'
    FIELD = r'[a-zA-Z_][a-zA-Z0-9_]*'
    COMMA = ','
    COLON = ':'
    LPAREN = r'\('
    RPAREN = r'\)'
    LBRACKET = '<'
    RBRACKET = '>'
    STRARG = _STRING_REGEX
    UNIT = '|'.join(dt.Interval._units.keys())


lexer = DataTypeLexer()

if __name__ == '__main__':
    pprint(list(lexer.tokenize('array<array<map<string: double>>>')))
