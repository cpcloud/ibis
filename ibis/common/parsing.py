from __future__ import annotations

import ast
import re
from functools import partial

import parsy

_STRING_REGEX = (
    """('[^\n'\\\\]*(?:\\\\.[^\n'\\\\]*)*'|"[^\n"\\\\"]*(?:\\\\.[^\n"\\\\]*)*")"""
)

SPACES = parsy.regex(r'\s*', re.MULTILINE)


def spaceless(parser):
    return SPACES.then(parser).skip(SPACES)


def spaceless_string(*strings: str):
    return spaceless(
        parsy.alt(*map(partial(parsy.string, transform=str.lower), strings))
    )


SINGLE_DIGIT = parsy.decimal_digit.desc("single digit")
RAW_NUMBER = SINGLE_DIGIT.at_least(1).concat().desc("decimal number")
PRECISION = SCALE = NUMBER = RAW_NUMBER.map(int)

LPAREN = spaceless_string("(").desc("left parenthesis")
RPAREN = spaceless_string(")").desc("right parenthesis")

LBRACKET = spaceless_string("[").desc("left square bracket [")
RBRACKET = spaceless_string("]").desc("right square bracket ]")

LANGLE = spaceless_string("<").desc("left angle bracket")
RANGLE = spaceless_string(">").desc("right angle bracket")

COMMA = spaceless_string(",").desc("comma")
COLON = spaceless_string(":").desc("colon")
SEMICOLON = spaceless_string(";").desc("semicolon")

RAW_STRING = parsy.regex(_STRING_REGEX).map(ast.literal_eval).desc("string")
FIELD = parsy.regex("[a-zA-Z_][a-zA-Z_0-9]*").desc("field name")
