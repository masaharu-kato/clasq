"""
"""

from enum import Enum
from telnetlib import BINARY
from typing import Optional

from .expr_class import ExprABC as Ext, UnaryOp, BinaryOp

class OP:
    ADD      = BinaryOp(b'+', [Ext, Ext], Ext, 7)
    SUB      = BinaryOp(b'-', [Ext, Ext], Ext, 7)
    MUL      = BinaryOp(b'*', [Ext, Ext], Ext, 6)
    DIV      = BinaryOp(b'/', [Ext, Ext], Ext, 6)
    MOD      = BinaryOp(b'%', [Ext, Ext], Ext, 6)
    MOD_     = BinaryOp(b'MOD', [Ext, Ext], Ext, 6)
    INTDIV   = BinaryOp(b'DIV', [Ext, Ext], Ext, 6)

    EQ       = BinaryOp(b'=', [Ext, Ext], Optional[bool], 11)
    IS       = BinaryOp(b'IS', [Ext, Ext], Optional[bool], 11)
    IS_NOT   = BinaryOp(b'IS NOT', [Ext, Ext], Optional[bool])
    LT       = BinaryOp(b'<', [Ext, Ext], Optional[bool], 11)
    LT_EQ    = BinaryOp(b'<=', [Ext, Ext], Optional[bool], 11)
    NULL_EQ  = BinaryOp(b'<=>', [Ext, Ext], Optional[bool], 11)
    GT       = BinaryOp(b'>', [Ext, Ext], Optional[bool], 11)
    GT_EQ    = BinaryOp(b'>=', [Ext, Ext], Optional[bool], 11)
    NOT_EQ   = BinaryOp(b'!=', [Ext, Ext], Optional[bool], 11)
    NOT_EQ_  = BinaryOp(b'<>', [Ext, Ext], Optional[bool], 11)

    BIT_AND_OP  = BinaryOp(b'&', [Ext, Ext], Ext, 9)
    BIT_OR_OP   = BinaryOp(b'|', [Ext, Ext], Ext, 10)
    BIT_XOR_OP  = BinaryOp(b'^', [Ext, Ext], Ext, 5)
    BIT_RSHIFT  = BinaryOp(b'>>', [Ext, Ext], Ext, 8)
    BIT_LSHIFT  = BinaryOp(b'<<', [Ext, Ext], Ext, 8)

    IN       = BinaryOp(b'IN', [Ext, Ext], Optional[bool], 11)
    LIKE     = BinaryOp(b'LIKE', [Ext, Ext], Optional[bool])
    AND  = BinaryOp(b'AND', [Ext, Ext], Optional[bool], 14)
    AND_ = BinaryOp(b'&&', [Ext, Ext], Optional[bool], 14)
    OR   = BinaryOp(b'OR', [Ext, Ext], Optional[bool], 16)
    OR_  = BinaryOp(b'||', [Ext, Ext], Optional[bool], 16)
    XOR  = BinaryOp(b'XOR', [Ext, Ext], Optional[bool], 15)
    
    RLIKE    = BinaryOp(b'RLIKE', [Ext, Ext], Optional[bool], 11)
    REGEXP   = BinaryOp(b'REGEXP', [Ext, Ext], Optional[bool], 11)
    SOUNDS_LIKE = BinaryOp(b'SOUNDS_LIKE', [Ext, Ext], Optional[bool], 11)

    COLLATE = BinaryOp(b'COLLATE', [Ext, Ext], Ext, 2)
    
    BETWEEN = BinaryOp(b'BETWEEN', [Ext, Ext, Ext], Ext, 12), # TODO: Special
    CASE    = BinaryOp(b'CASE', [Ext, Ext, Ext], Ext, 12), # TODO:: Special
    
    MINUS    = UnaryOp(b'-', [Ext], Ext, 4)

    BIT_INV  = UnaryOp(b'~', [Ext], Ext, 4)
    NOT_OP   = UnaryOp(b'!', [Ext], Ext, 3)

    NOT      = UnaryOp(b'NOT', [Ext], Ext, 13)
    BINARY   = UnaryOp(b'BINARY', [Ext], Ext, 2)

    JSON_EXTRACT_OP = BinaryOp(b'->', [Ext, Ext], str)
    JSON_UNQUOTE_OP = BinaryOp(b'->>', [Ext, Ext], str)

    MEMBER_OF = BinaryOp(b'MEMBER OF', [Ext, Ext], Optional[bool], 11)
