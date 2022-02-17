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

parser grammar RelationalParser;

import IdentifiersParser;

options
{
    tokenVocab=RelationalLexer;
}

//Starting rule
statements
    :
        (statement | empty_)
        (statementSeparator statement | empty_)* EOF
    ;

empty_
   :
    statementSeparator
   ;

statementSeparator
    :
        SEMICOLON
    ;

statement
  	:
  	explainStatement | execStatement
   	;

execStatement
    :
        createTemplateStatement
        | createSchemaStatement
        | createDatabaseStatement
        | createTableStatement
        | createValueIndexStatement
    ;

explainStatement
   	: KW_EXPLAIN ( execStatement )
   	;

createTemplateStatement
    :
        KW_CREATE (KW_SCHEMA KW_TEMPLATE) identifier
        (KW_AS LCURLY
            schemaStatement
            (statementSeparator schemaStatement)*
        RCURLY)
        ((KW_WITH KW_PROPERTIES) dbProperties)?
    ;

schemaStatement
    :
        createTableStatement
        | createValueIndexStatement
    ;

createSchemaStatement
    : KW_CREATE KW_SCHEMA
    identifier
    (KW_ON KW_DATABASE)
    identifier
    (KW_FROM KW_TEMPLATE)
    identifier
    ((KW_WITH KW_PROPERTIES) dbProperties)?
    ;

//TODO(bfines) this isn't finished
createDatabaseStatement
    :
        KW_CREATE (KW_INDEX | KW_DATABASE) identifier ((KW_WITH KW_PROPERTIES) dbProperties)?
    ;

createTableStatement
    :
        KW_CREATE KW_TABLE identifier
        (tableColumnList | recordType)
        ((KW_WITH KW_PROPERTIES) dbProperties)?
    ;

recordType
    :
        KW_USING KW_DESCRIPTOR StringLiteral //identifier here is the fully-qualified class name for the RecordType to define
    ;

tableColumnList
    :
        LPAREN
            tableColumn
            (COMMA tableColumn)*
        RPAREN
    ;

createValueIndexStatement
    :
        KW_CREATE (KW_UNIQUE| KW_NONQUERYABLE)? KW_VALUE KW_INDEX identifier KW_ON identifier
        LPAREN
            valueIndexColumn
            (COMMA valueIndexColumn)*
        RPAREN
        ((KW_WITH KW_PROPERTIES) dbProperties)?
    ;

valueIndexColumn
    :
        identifier //here we add extra column options like FAN_OUT, NONNULL, UNIQUE, etc
    ;
tableColumn
    :
        identifier dataType (KW_REPEATED | KW_PRIMARYKEY)?
    ;

dataType
    :
        KW_STRING
        | KW_LONG
        | KW_DOUBLE
        | KW_MESSAGE
    ;
