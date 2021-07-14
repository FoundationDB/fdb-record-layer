/*
MySQL (Positive Technologies) grammar
The MIT License (MIT).
Copyright (c) 2015-2017, Ivan Kochurkin (kvanttt@gmail.com), Positive Technologies.
Copyright (c) 2017, Ivan Khudyashev (IHudyashov@ptsecurity.com)
Copyright 2021-2024 Apple Inc. and the FoundationDB project authors

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
*/

lexer grammar RelationalLexer;

// connecting keywords
KW_WITH: 'WITH';
KW_FROM: 'FROM';
KW_ON: 'ON';
KW_AS: 'AS';
KW_USING: 'USING';

//action keywords
KW_CREATE: 'CREATE';
KW_EXPLAIN: 'EXPLAIN';

//action type keywords
KW_TEMPLATE: 'TEMPLATE';
KW_SCHEMA: 'SCHEMA';
KW_DATABASE: 'DATABASE';
KW_TABLE: 'TABLE';
KW_INDEX: 'INDEX';

//data type keywords
KW_DESCRIPTOR: 'DESCRIPTOR';
KW_STRING: 'STRING';
KW_LONG: 'LONG';
KW_DOUBLE: 'DOUBLE';
KW_REPEATED: 'REPEATED';
KW_MESSAGE: 'MESSAGE';
KW_PROPERTIES: 'PROPERTIES';

//Table Keywords
KW_PRIMARYKEY: ('PRIMARY KEY');

//index keywords
KW_VALUE: 'VALUE';
KW_TEXT: 'TEXT';
KW_AGGREGATE: 'AGGREGATE';
KW_UNIQUE: 'UNIQUE';
KW_NONQUERYABLE: 'NONQUERYABLE';

DOT : '.';
COLON : ':' ;
COMMA : ',' ;
SEMICOLON : ';' ;

LPAREN : '(' ;
RPAREN : ')' ;
LSQUARE : '[' ;
RSQUARE : ']' ;
LCURLY : '{';
RCURLY : '}';

EQUAL : '=' | '==';

fragment
Letter
    : 'a'..'z' | 'A'..'Z'
    ;

fragment
Digit
    : '0'..'9'
    ;

StringLiteral
    : ( '\'' ( ~('\''|'\\') | ('\\' .) )* '\''
    | '"' ( ~('"'|'\\') | ('\\' .) )* '"'
    )+
    ;

Identifier
    : (Letter | Digit) (Letter | Digit| '_')+
    | QuotedIdentifier
    ;

QuotedIdentifier
    :
    '`' ('``'|~('`'))* '`'
    ;

//ignore whitespace
WS  : (' '|'\r'|'\t'|'\n') -> channel(HIDDEN) ;

