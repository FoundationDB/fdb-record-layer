/*
A possibly temporary parser for a simple DDL language. This may be ultimately merged with the larger Relational
parser, but at the time this was written that parser didn't exist yet.
*/

parser grammar DdlParser;

options
{
    tokenVocab=DdlLexer;
}

identifier: Identifier;

//the actual entrypoint statement
statements
    :
        (statement | empty_)
        (statementSeparator statement | empty_)* EOF
    ;

statementSeparator
    :
        SEMICOLON
    ;

empty_
    :
        statementSeparator
    ;

statement
    :
        createStatement
        | dropStatement
        | showStatement
        | describeStatement
    ;

createStatement
    :
        KW_CREATE (schemaDef | schemaTemplateDef | databaseIdentifier)
     ;

indexDef
    :
        valueIndexDef
    ;

valueIndexDef
    :
        KW_UNIQUE? KW_VALUE KW_INDEX identifier KW_ON identifier LPAREN
        ( identifier (COMMA identifier)*)
        RPAREN
        ( KW_INCLUDE LPAREN (identifier (COMMA identifier)*) RPAREN )?
    ;

schemaTemplateDef
    :
        (KW_SCHEMA KW_TEMPLATE) identifier (
                    KW_AS LCURLY
                        (KW_CREATE (structDef | indexDef))
                        (statementSeparator (KW_CREATE (structDef | indexDef))?)*
                    RCURLY
                )
    ;

structDef
    :
        (KW_STRUCT |KW_TABLE)  identifier  (LPAREN
                             columnDef
                             (COMMA columnDef)*
                             (primaryKeyDef)?
                        RPAREN)
    ;

primaryKeyDef
    :
        (KW_PRIMARY KW_KEY) LPAREN (KW_RECORD_TYPE | identifier) (COMMA identifier)* RPAREN
    ;

schemaDef
    :
        KW_SCHEMA schemaIdentifier KW_WITH templateIdentifier
    ;

columnDef
    :
        identifier
        ((KW_BOOLEAN | KW_INT64 | KW_DOUBLE | KW_STRING | KW_TIMESTAMP | KW_BYTES| identifier) KW_ARRAY?)
    ;

databaseIdentifier
    :
        KW_DATABASE Path
    ;

schemaTemplateIdentifier
    :
        KW_SCHEMA KW_TEMPLATE identifier
    ;

dropStatement
    :
        KW_DROP (databaseIdentifier | schemaOrTemplateIdentifier)
     ;

/*
 * Syntax:
 *  SHOW DATABASES --> Show all databases
 *  SHOW DATABASES WITH PREFIX Path --> Show all databases with a shared prefix
 */

showStatement
    :
       KW_SHOW (showDatabases | showTemplates)
    ;

showDatabases
    :
        KW_DATABASES (KW_WITH KW_PREFIX Path)?
    ;

showTemplates
    :
        KW_SCHEMA KW_TEMPLATES
    ;

describeStatement
    :
        KW_DESCRIBE (schemaOrTemplateIdentifier)
    ;

schemaOrTemplateIdentifier
    :
        KW_SCHEMA ( schemaIdentifier | templateIdentifier)
    ;

templateIdentifier
    :
        KW_TEMPLATE identifier
    ;

schemaIdentifier
    :
        (Path | identifier)
    ;
