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

options { tokenVocab=RelationalLexer; }

// Top Level Description

root
    : statements? (MINUS MINUS)? EOF
    ;

statements
    : statement (SEMI statement)* SEMI?
    ;

statement
    : selectStatement
    | ddlStatement
    | dmlStatement
    | transactionStatement
    | preparedStatement
    | administrationStatement
    | utilityStatement
    ;

dmlStatement
    : insertStatement
    | updateStatement
    | deleteStatement
    ;

ddlStatement
    : createStatement
    | dropStatement
    ;

transactionStatement
    : startTransaction
    | commitStatement
    | rollbackStatement
    ;

preparedStatement
    : prepareStatement | executeStatement 
    ;

administrationStatement
    : setStatement 
    | showStatement 
    | killStatement
    | resetStatement
    | executeContinuationStatement
    ;

utilityStatement
    : simpleDescribeStatement 
    | fullDescribeStatement
    | helpStatement 
    ;


// Data Definition Language

templateClause
    :
        CREATE ( structDefinition | tableDefinition | enumDefinition | indexDefinition | functionDefinition)
    ;

createStatement
   : CREATE SCHEMA schemaId WITH TEMPLATE schemaTemplateId                                            #createSchemaStatement
   | CREATE SCHEMA TEMPLATE schemaTemplateId  templateClause+ optionsClause?                          #createSchemaTemplateStatement
   | CREATE DATABASE path                                                                             #createDatabaseStatement
   ;

optionsClause
    : WITH OPTIONS '(' option (COMMA option)* ')'
    ;

option
    : ENABLE_LONG_ROWS EQUAL_SYMBOL booleanLiteral
    | INTERMINGLE_TABLES EQUAL_SYMBOL booleanLiteral
    | STORE_ROW_VERSIONS EQUAL_SYMBOL booleanLiteral
    ;

dropStatement
   : DROP DATABASE ifExists? path       #dropDatabaseStatement
   | DROP SCHEMA TEMPLATE ifExists? uid #dropSchemaTemplateStatement
   | DROP SCHEMA ifExists? uid          #dropSchemaStatement
   ;

// details

structDefinition
    : (TYPE AS STRUCT) uid LEFT_ROUND_BRACKET columnDefinition (COMMA columnDefinition)* RIGHT_ROUND_BRACKET
    ;

tableDefinition
    : TABLE uid LEFT_ROUND_BRACKET columnDefinition (COMMA columnDefinition)* COMMA primaryKeyDefinition RIGHT_ROUND_BRACKET
    ;

columnDefinition
    : colName=uid columnType ARRAY? columnConstraint?
    ;

columnType
    : primitiveType | customType=uid;

primitiveType
    : BOOLEAN | INTEGER | BIGINT | FLOAT | DOUBLE | STRING | BYTES;

columnConstraint
    : nullNotnull                                                   #nullColumnConstraint
    ;

primaryKeyDefinition
    : PRIMARY KEY fullIdList
    | SINGLE ROW ONLY
    ;

fullIdList
    : '(' fullId (COMMA fullId)* ')'
    ;

enumDefinition
    : TYPE AS ENUM uid '(' STRING_LITERAL (COMMA STRING_LITERAL)* ')'
    ;

indexDefinition
    : (UNIQUE)? INDEX indexName=uid AS queryTerm indexAttributes?
    ;

functionDefinition
    : FUNCTION functionName=uid LEFT_ROUND_BRACKET paramName=uid inputTypeName=columnType RIGHT_ROUND_BRACKET RETURNS columnType AS fullId
    ;

indexAttributes
    : WITH ATTRIBUTES indexAttribute (COMMA indexAttribute)*
    ;

indexAttribute
    : LEGACY_EXTREMUM_EVER
    ;

charSet
    : CHARACTER SET
    | CHARSET
    | CHAR SET
    ;

intervalType
    : intervalTypeBase
    | YEAR | YEAR_MONTH | DAY_HOUR | DAY_MINUTE
    | DAY_SECOND | HOUR_MINUTE | HOUR_SECOND | MINUTE_SECOND
    | SECOND_MICROSECOND | MINUTE_MICROSECOND
    | HOUR_MICROSECOND | DAY_MICROSECOND
    ;

schemaId: // redundant, remove.
   path;

path: // redundant, remove.
   uid;

schemaTemplateId: // redundant, remove.
   uid;

// Data Manipulation Language

//    Primary DML Statements

deleteStatement
    : DELETE
      FROM tableName
      (WHERE whereExpr)?
      orderByClause? limitClause?
      (RETURNING selectElements)?
      queryOptions?
    ;

insertStatement
    : INSERT
      INTO? tableName
      (columns=uidListWithNestingsInParens)? insertStatementValue
      queryOptions?
    ;

continuationAtom
    : bytesLiteral
    | preparedStatementParameter
    ;

selectStatement
    : query
    ;

query
    : ctes? queryExpressionBody continuation?
    ;

ctes
    : WITH (RECURSIVE)? namedQuery (COMMA namedQuery)*
    ;

namedQuery
    : name=fullId (columnAliases=fullIdList)? AS? '(' query ')'
    ;

continuation
    : WITH CONTINUATION continuationAtom
    ;

// done
queryExpressionBody
    : queryTerm                                                                                             #queryTermDefault // done
    | left=queryExpressionBody operator=UNION quantifier=(ALL | DISTINCT)? right=queryExpressionBody         #setQuery // done (TODO add more operations)
    ;

// details

insertStatementValue
    : queryExpressionBody                                               #insertStatementValueSelect
    | insertFormat=(VALUES | VALUE)
      recordConstructorForInsert (',' recordConstructorForInsert )*     #insertStatementValueValues
    ;

updatedElement
    : fullColumnName '=' (expression | DEFAULT)
    ;

assignmentField
    : uid | LOCAL_ID
    ;

//    Detailed DML Statements

updateStatement
    : UPDATE tableName (AS? uid)?
      SET updatedElement (',' updatedElement)*
      (WHERE whereExpr)?
      (RETURNING selectElements)?
      (WITH CONTINUATION continuationAtom)?
      queryOptions?
    ;

// details

orderByClause
    : ORDER BY orderByExpression (',' orderByExpression)*
    ;

orderByExpression
    : expression order=(ASC | DESC)? (NULLS nulls=(FIRST | LAST))?
    ;

tableSources // done
    : tableSource (',' tableSource)*
    ;

tableSource // done
    : tableSourceItem joinPart*                                     #tableSourceBase // done
    ;

tableSourceItem // done
    : tableName (AS? alias=uid)? (indexHint (',' indexHint)* )?    #atomTableItem // done
    | query AS? alias=uid                                          #subqueryTableItem // done
    ;

indexHint
    : indexHintAction=(USE | IGNORE | FORCE)
      keyFormat=(INDEX|KEY) ( FOR indexHintType)?
      '(' uidList ')'
    ;

indexHintType
    : JOIN | ORDER BY | GROUP BY
    ;

joinPart
    : (INNER | CROSS)? JOIN tableSourceItem
      (
        ON expression
        | USING '(' uidList ')'
      )?                                                            #innerJoin
    | STRAIGHT_JOIN tableSourceItem (ON expression)?                #straightJoin
    | (LEFT | RIGHT) OUTER? JOIN tableSourceItem
        (
          ON expression
          | USING '(' uidList ')'
        )                                                           #outerJoin
    | NATURAL ((LEFT | RIGHT) OUTER?)? JOIN tableSourceItem         #naturalJoin
    ;

//    Select Statement's Details

// done
queryTerm
    : SELECT DISTINCT?
    selectElements
    fromClause?
    groupByClause?
    havingClause?
    /*windowClause?*/
    orderByClause?
    limitClause?
    queryOptions?                                                  #simpleTable
    | '(' query ')'                                                #parenthesisQuery
    ;

// details

selectElements // done
    : selectElement (',' selectElement)*
    ;

// done
selectElement
    : STAR                      #selectStarElement // done
    | uid DOT STAR              #selectQualifierStarElement // done
    | expression (AS? uid)?     #selectExpressionElement // done
    ;

fromClause // done
    : FROM tableSources (WHERE whereExpr)?
    ;

groupByClause // done
    :  GROUP BY
       groupByItem (',' groupByItem)*
    ;

whereExpr
    : expression;

havingClause
    :  HAVING havingExpr=expression
    ;

//commenting out Windows, because we'll want them eventually, but don't want to deal with them now
// windowClause
//     :  WINDOW windowName AS '(' windowSpec ')' (',' windowName AS '(' windowSpec ')')*
//     ;

groupByItem
    : expression (AS? uid)? order=(ASC | DESC)? // in Relational we support named grouping columns.
    ;

// done
limitClause
    : LIMIT limit=limitClauseAtom (OFFSET offset=limitClauseAtom)?
    ;

limitClauseAtom
    : decimalLiteral
    | preparedStatementParameter
    ;

queryOptions
    : OPTIONS '(' queryOption (',' queryOption)* ')'
    ;

queryOption
    : NOCACHE
    | LOG QUERY
    | DRY RUN
    | CONTINUATION CONTAINS COMPILED STATEMENT
    ;

// Transaction's Statements

startTransaction
    : START TRANSACTION 
    ;


commitStatement
    : COMMIT 
    ;

rollbackStatement
    : ROLLBACK 
    ;

// details

setAutocommitStatement
    : SET AUTOCOMMIT '=' autocommitValue=(ON | OFF)
    ;

setTransactionStatement
    : SET transactionContext=(GLOBAL | SESSION)? TRANSACTION
      transactionOption (',' transactionOption)*
    ;

transactionOption
    : ISOLATION LEVEL transactionLevel
    ;

transactionLevel
    : READ COMMITTED
    | SERIALIZABLE
    ;

// Prepared Statements

prepareStatement
    : PREPARE uid FROM
      (queryString=STRING_LITERAL | variable=LOCAL_ID)
    ;

executeStatement
    : EXECUTE uid (USING userVariables)?
    ;


//    Set and show statements

//show databases and schema templates
showStatement
    : SHOW DATABASES (WITH PREFIX path)? #showDatabasesStatement
    | SHOW SCHEMA TEMPLATES              #showSchemaTemplatesStatement
    ;

setStatement
    : SET variableClause ('=' | ':=') expression
      (',' variableClause ('=' | ':=') expression)*                             #setVariable
    | SET charSet (charsetName | DEFAULT)          #setCharset
    | SET NAMES
        (charsetName (COLLATE collationName)? | DEFAULT)                        #setNames
    | setTransactionStatement                                                   #setTransaction
    | setAutocommitStatement                                                    #setAutocommit
    | SET fullId ('=' | ':=') expression
      (',' fullId ('=' | ':=') expression)*                                     #setNewValueInsideTrigger
    ;


// details

variableClause
    : LOCAL_ID | ( ('@' '@')? (GLOBAL | SESSION | LOCAL)  )? uid
    ;

//    Other administrative statements

killStatement
    : KILL connectionFormat=(CONNECTION | QUERY)?
      decimalLiteral+
    ;

resetStatement
    : RESET QUERY CACHE
    ;

executeContinuationStatement
    : EXECUTE CONTINUATION packageBytes=continuationAtom
      queryOptions?
    ;

// details

tableIndexes
    : tableName ( indexFormat=(INDEX | KEY)? '(' uidList ')' )?
    ;

loadedTableIndexes
    : tableName
      ( PARTITION '(' (partitionList=uidList | ALL) ')' )?
      ( indexFormat=(INDEX | KEY)? '(' indexList=uidList ')' )?
      (IGNORE LEAVES)?
    ;


// Utility Statements


simpleDescribeStatement
    : command=(EXPLAIN | DESCRIBE | DESC) SCHEMA schemaId     #simpleDescribeSchemaStatement
    | command=(EXPLAIN | DESCRIBE | DESC) SCHEMA TEMPLATE uid #simpleDescribeSchemaTemplateStatement
    ;

// TODO: implement full describe for schema and schema template.
fullDescribeStatement
    : command=(EXPLAIN | DESCRIBE | DESC)
      (
        formatType=(EXTENDED | PARTITIONS | FORMAT )
        '='
        formatValue=(TRADITIONAL | JSON)
      )?
      describeObjectClause
    ;

helpStatement
    : HELP STRING_LITERAL
    ;

// details

describeObjectClause
    : (
        query | deleteStatement | insertStatement
        | updateStatement | executeContinuationStatement
      )                                                             #describeStatements
    | FOR CONNECTION uid                                            #describeConnection
    ;

// done
fullId
    : uid (DOT uid)*
    ;

// done
tableName
    : fullId
    ;

// done
fullColumnName
    : fullId
    ;

// done (unsupported)
indexColumnName
    : ((uid | STRING_LITERAL) ('(' decimalLiteral ')')? | expression) sortType=(ASC | DESC)?
    ;

// done (unsupported)
charsetName
    : BINARY
    | charsetNameBase
    | STRING_LITERAL
    ;

// done (unsupported)
collationName
    : uid | STRING_LITERAL;

// done
uid
    : simpleId
    | DOUBLE_QUOTE_ID
    ;

// done
simpleId
    : ID
    | charsetNameBase
    | intervalTypeBase
    | keywordsCanBeId
    | functionNameBase
    ;

//    Literals

nullNotnull
    : NOT? NULL_LITERAL
    ;

// done
decimalLiteral
    : DECIMAL_LITERAL | REAL_LITERAL
    ;

// done
stringLiteral
    : (
        STRING_CHARSET_NAME? STRING_LITERAL
        | START_NATIONAL_STRING_LITERAL
      ) STRING_LITERAL+
    | (
        STRING_CHARSET_NAME? STRING_LITERAL
        | START_NATIONAL_STRING_LITERAL
      ) (COLLATE collationName)?
    ;

// done
booleanLiteral
    : TRUE | FALSE;

// done. check X'0A0B...' literal syntax.
bytesLiteral
    : HEXADECIMAL_LITERAL | BASE64_LITERAL;

// done
nullLiteral
    : NULL_LITERAL;

// done
constant
    : stringLiteral       #stringConstant // done
    | decimalLiteral      #decimalConstant // done
    | '-' decimalLiteral  #negativeDecimalConstant // done
    | bytesLiteral        #bytesConstant // done
    | booleanLiteral      #booleanConstant // done
    | BIT_STRING          #bitStringConstant // done (unsupported)
    | NOT? nullLiteral    #nullConstant // done (unsupported) - if NOT exists.
    ;


//    Data Types

dataType
    : typeName=(
      CHAR | CHARACTER | VARCHAR | TINYTEXT | TEXT | MEDIUMTEXT | LONGTEXT
       | NCHAR | NVARCHAR | LONG
      )
      VARYING?
      lengthOneDimension? BINARY?
      (charSet charsetName)?
      (COLLATE collationName | BINARY)?                             #stringDataType
    | NATIONAL typeName=(VARCHAR | CHARACTER)
      lengthOneDimension? BINARY?                                   #nationalStringDataType
    | NCHAR typeName=VARCHAR
      lengthOneDimension? BINARY?                                   #nationalStringDataType
    | NATIONAL typeName=(CHAR | CHARACTER) VARYING
      lengthOneDimension? BINARY?                                   #nationalVaryingStringDataType
    | typeName=(
        TINYINT | SMALLINT | MEDIUMINT | INT | INTEGER | BIGINT
        | MIDDLEINT | INT1 | INT2 | INT3 | INT4 | INT8
      )
      lengthOneDimension? (SIGNED | UNSIGNED | ZEROFILL)*           #dimensionDataType
    | typeName=REAL
      lengthTwoDimension? (SIGNED | UNSIGNED | ZEROFILL)*           #dimensionDataType
    | typeName=DOUBLE PRECISION?
      lengthTwoDimension? (SIGNED | UNSIGNED | ZEROFILL)*           #dimensionDataType
    | typeName=(DECIMAL | DEC | FIXED | NUMERIC | FLOAT | FLOAT4 | FLOAT8)
      lengthTwoOptionalDimension? (SIGNED | UNSIGNED | ZEROFILL)*   #dimensionDataType
    | typeName=(
        DATE | TINYBLOB | MEDIUMBLOB | LONGBLOB
        | BOOL | BOOLEAN | SERIAL
      )                                                             #simpleDataType
    | typeName=(
        BIT | TIME | TIMESTAMP | DATETIME | BINARY
        | VARBINARY | BLOB | YEAR
      )
      lengthOneDimension?                                           #dimensionDataType
    | typeName=(ENUM | SET)
      collectionOptions BINARY?
      (charSet charsetName)?                                        #collectionDataType
    | typeName=(
        GEOMETRYCOLLECTION | GEOMCOLLECTION | LINESTRING | MULTILINESTRING
        | MULTIPOINT | MULTIPOLYGON | POINT | POLYGON | JSON | GEOMETRY
      )                                                             #spatialDataType
    | typeName=LONG VARCHAR?
      BINARY?
      (charSet charsetName)?
      (COLLATE collationName)?                                      #longVarcharDataType    // LONG VARCHAR is the same as LONG
    | LONG VARBINARY                                                #longVarbinaryDataType
    ;

collectionOptions
    : '(' STRING_LITERAL (',' STRING_LITERAL)* ')'
    ;

convertedDataType
    :
    (
      typeName=(BINARY| NCHAR) lengthOneDimension?
      | typeName=CHAR lengthOneDimension? (charSet charsetName)?
      | typeName=(DATE | DATETIME | TIME | JSON | INT | INTEGER)
      | typeName=DECIMAL lengthTwoOptionalDimension?
      | (SIGNED | UNSIGNED) INTEGER?
    ) ARRAY?
    ;

lengthOneDimension
    : '(' decimalLiteral ')'
    ;

lengthTwoDimension
    : '(' decimalLiteral ',' decimalLiteral ')'
    ;

lengthTwoOptionalDimension
    : '(' decimalLiteral (',' decimalLiteral)? ')'
    ;


//    Common Lists

uidList
    : uid (',' uid)*
    ;

uidWithNestings
    : uid uidListWithNestingsInParens?
    ;

uidListWithNestingsInParens
    : '(' uidListWithNestings ')'
    ;

uidListWithNestings
    : uidWithNestings (',' uidWithNestings)*
    ;

tables
    : tableName (',' tableName)*
    ;

indexColumnNames
    : '(' indexColumnName (',' indexColumnName)* ')'
    ;

expressions
    : expression (',' expression)*
    ;

expressionsWithDefaults
    : expressionOrDefault (',' expressionOrDefault)*
    ;

recordConstructorForInsert
    : LEFT_ROUND_BRACKET expressionWithOptionalName (',' expressionWithOptionalName)* RIGHT_ROUND_BRACKET
    ;

recordConstructor
    : ofTypeClause? LEFT_ROUND_BRACKET (uid DOT STAR | STAR | expressionWithName | expressionWithOptionalName (',' expressionWithOptionalName)*) RIGHT_ROUND_BRACKET
    ;

ofTypeClause
    : STRUCT uid
    ;

arrayConstructor
    : LEFT_SQUARE_BRACKET expression (',' expression)* RIGHT_SQUARE_BRACKET
    ;

userVariables
    : LOCAL_ID (',' LOCAL_ID)*
    ;


//    Common Expressons

defaultValue
    : (NULL_LITERAL | unaryOperator? constant | currentTimestamp | '(' expression ')') (ON UPDATE currentTimestamp)?
    ;

currentTimestamp
    :
    (
      (CURRENT_TIMESTAMP | LOCALTIME | LOCALTIMESTAMP) ('(' decimalLiteral? ')')?
      | NOW '(' decimalLiteral? ')'
    )
    ;

expressionOrDefault
    : expression | DEFAULT
    ;

expressionWithName
    : expression AS uid
    ;

expressionWithOptionalName
    : expression (AS uid)?
    ;

ifExists
    : IF EXISTS;

ifNotExists
    : IF NOT EXISTS;


//    Functions

functionCall
    : aggregateWindowedFunction                                     #aggregateFunctionCall // done (supported)
    | specificFunction                                              #specificFunctionCall //
    | scalarFunctionName '(' functionArgs? ')'                      #scalarFunctionCall // done (unsupported)
    | userDefinedFunctionName=uid '(' functionArgs? ')'             #userDefinedFunctionCall
    ;

specificFunction
    : (
      CURRENT_DATE | CURRENT_TIME | CURRENT_TIMESTAMP
      | CURRENT_USER | LOCALTIME
      ) ('(' ')')?                                                  #simpleFunctionCall
    | CONVERT '(' expression separator=',' convertedDataType ')'    #dataTypeFunctionCall
    | CONVERT '(' expression USING charsetName ')'                  #dataTypeFunctionCall
    | CAST '(' expression AS convertedDataType ')'                  #dataTypeFunctionCall
    | VALUES '(' fullColumnName ')'                                 #valuesFunctionCall
    | CASE expression caseFuncAlternative+
      (ELSE elseArg=functionArg)? END                               #caseExpressionFunctionCall
    | CASE caseFuncAlternative+
      (ELSE elseArg=functionArg)? END                               #caseFunctionCall
    | CHAR '(' functionArgs  (USING charsetName)? ')'               #charFunctionCall
    | POSITION
      '('
          (
            positionString=stringLiteral
            | positionExpression=expression
          )
          IN
          (
            inString=stringLiteral
            | inExpression=expression
          )
      ')'                                                           #positionFunctionCall
    | (SUBSTR | SUBSTRING)
      '('
        (
          sourceString=stringLiteral
          | sourceExpression=expression
        ) FROM
        (
          fromDecimal=decimalLiteral
          | fromExpression=expression
        )
        (
          FOR
          (
            forDecimal=decimalLiteral
            | forExpression=expression
          )
        )?
      ')'                                                           #substrFunctionCall
    | TRIM
      '('
        positioinForm=(BOTH | LEADING | TRAILING)
        (
          sourceString=stringLiteral
          | sourceExpression=expression
        )?
        FROM
        (
          fromString=stringLiteral
          | fromExpression=expression
        )
      ')'                                                           #trimFunctionCall
    | TRIM
      '('
        (
          sourceString=stringLiteral
          | sourceExpression=expression
        )
        FROM
        (
          fromString=stringLiteral
          | fromExpression=expression
        )
      ')'                                                           #trimFunctionCall
    | WEIGHT_STRING
      '('
        (stringLiteral | expression)
        (AS stringFormat=(CHAR | BINARY)
        '(' decimalLiteral ')' )?  levelsInWeightString?
      ')'                                                           #weightFunctionCall
    | EXTRACT
      '('
        intervalType
        FROM
        (
          sourceString=stringLiteral
          | sourceExpression=expression
        )
      ')'                                                           #extractFunctionCall
    | GET_FORMAT
      '('
        datetimeFormat=(DATE | TIME | DATETIME)
        ',' stringLiteral
      ')'                                                           #getFormatFunctionCall
    ;

caseFuncAlternative
    : WHEN condition=functionArg
      THEN consequent=functionArg
    ;

levelsInWeightString
    : LEVEL levelInWeightListElement
      (',' levelInWeightListElement)*                               #levelWeightList
    | LEVEL
      firstLevel=decimalLiteral '-' lastLevel=decimalLiteral        #levelWeightRange
    ;

levelInWeightListElement
    : decimalLiteral orderType=(ASC | DESC | REVERSE)?
    ;

aggregateWindowedFunction
    : functionName=(AVG | MAX | MIN | SUM | MAX_EVER | MIN_EVER )
      '(' aggregator=(ALL | DISTINCT)? functionArg ')' overClause?
    | functionName=BITMAP_CONSTRUCT_AGG '(' functionArg ')'
    | functionName=COUNT '(' (starArg='*' | aggregator=ALL? functionArg | aggregator=DISTINCT functionArgs) ')' overClause?
    | functionName=(
        BIT_AND | BIT_OR | BIT_XOR | STD | STDDEV | STDDEV_POP
        | STDDEV_SAMP | VAR_POP | VAR_SAMP | VARIANCE
      ) '(' aggregator=ALL? functionArg ')' overClause?
    | functionName=GROUP_CONCAT '('
        aggregator=DISTINCT? functionArgs
        (ORDER BY
          orderByExpression (',' orderByExpression)*
        )? (SEPARATOR separator=STRING_LITERAL)?
      ')'
    ;

nonAggregateWindowedFunction
    : (LAG | LEAD) '(' expression (',' decimalLiteral)? (',' decimalLiteral)? ')' overClause
    | (FIRST_VALUE | LAST_VALUE) '(' expression ')' overClause
    | (CUME_DIST | DENSE_RANK | PERCENT_RANK | RANK | ROW_NUMBER) '('')' overClause
    | NTH_VALUE '(' expression ',' decimalLiteral ')' overClause
    | NTILE '(' decimalLiteral ')' overClause
    ;

overClause
    : OVER (/* '(' windowSpec? ')' |*/ windowName)
    ;

windowName
    : uid
    ;

//commented out until we want to support window functions
/* 
windowSpec
    : windowName? partitionClause? orderByClause? frameClause?
    ;


frameClause
    : frameUnits frameExtent
    ;

frameUnits
    : ROWS
    | RANGE
    ;

frameExtent
    : frameRange
    | frameBetween
    ;

frameBetween
    : BETWEEN frameRange AND frameRange
    ;

frameRange
    : CURRENT ROW
    | UNBOUNDED (PRECEDING | FOLLOWING)
    | expression (PRECEDING | FOLLOWING)
    ;

partitionClause
    : PARTITION BY expression (',' expression)*
    ;
*/

scalarFunctionName
    : functionNameBase
    | ASCII | CURDATE | CURRENT_DATE | CURRENT_TIME
    | CURRENT_TIMESTAMP | CURTIME | DATE_ADD | DATE_SUB
    | IF | INSERT | LOCALTIME | LOCALTIMESTAMP | MID | NOW
    | REPLACE | SUBSTR | SUBSTRING | SYSDATE | TRIM
    | UTC_DATE | UTC_TIME | UTC_TIMESTAMP
    | JAVA_CALL
    ;

functionArgs
    : functionArg ( ',' functionArg)*
    ;

functionArg
    : expression
    ;

//    Expressions, predicates

// Simplified approach for expression
expression
    : notOperator=(NOT | '!') expression                                                #notExpression     // done
    | expression logicalOperator expression                                             #logicalExpression // done
    | predicate IS NOT? testValue=(TRUE | FALSE | NULL_LITERAL)                         #isExpression      // done
    | predicate NOT? LIKE pattern=STRING_LITERAL (ESCAPE escape=STRING_LITERAL)?        #likePredicate // done
    | predicate                                                                         #predicateExpression // done
    ;

predicate
    : expressionAtom IN inList                                      #inPredicate // done
    | left=predicate comparisonOperator right=predicate             #binaryComparisonPredicate // done
    | expressionAtom                                                #expressionAtomPredicate // done
    ;

inList
    : '(' (queryExpressionBody | expressions) ')'
    | preparedStatementParameter
    ;

// Add in ASTVisitor nullNotnull in constant
expressionAtom
    : constant                                                      #constantExpressionAtom // done
    | fullColumnName                                                #fullColumnNameExpressionAtom // done
    | functionCall                                                  #functionCallExpressionAtom // done
    | preparedStatementParameter                                    #preparedStatementParameterAtom // done
    | recordConstructor                                             #recordConstructorExpressionAtom // done
    | arrayConstructor                                              #arrayConstructorExpressionAtom // done
    | EXISTS '(' query ')'                                          #existsExpressionAtom // done
    | '(' queryExpressionBody ')'                                   #subqueryExpressionAtom // done (unsupported)
    | INTERVAL expression intervalType                              #intervalExpressionAtom // done (unsupported)
    | left=expressionAtom bitOperator right=expressionAtom          #bitExpressionAtom // done
    | left=expressionAtom mathOperator right=expressionAtom         #mathExpressionAtom // done
    | left=expressionAtom jsonOperator right=expressionAtom         #jsonExpressionAtom // done (unsupported)
    ;

preparedStatementParameter
    : QUESTION
    | NAMED_PARAMETER
    ;

unaryOperator
    : '!' | '~' | '+' | '-' | NOT
    ;

comparisonOperator
    : '=' | '>' | '<' | '<' '=' | '>' '='
    | '<' '>' | '!' '=' // | '<' '=' '>' // no support for null-safe equality
    ;

logicalOperator
    : AND | '&' '&' | XOR | OR | '|' '|'
    ;

bitOperator
    : '<' '<' | '>' '>' | '&' | '^' | '|'
    ;

mathOperator
    : '*' | '/' | '%' | DIV | MOD | '+' | '-'
    ;

jsonOperator
    : '-' '>' | '-' '>' '>'
    ;

//    Simple id sets
//     (that keyword, which can be id)

charsetNameBase
    : ARMSCII8 | ASCII | BIG5 | BINARY | CP1250 | CP1251 | CP1256 | CP1257
    | CP850 | CP852 | CP866 | CP932 | DEC8 | EUCJPMS | EUCKR
    | GB18030 | GB2312 | GBK | GEOSTD8 | GREEK | HEBREW | HP8 | KEYBCS2
    | KOI8R | KOI8U | LATIN1 | LATIN2 | LATIN5 | LATIN7 | MACCE
    | MACROMAN | SJIS | SWE7 | TIS620 | UCS2 | UJIS | UTF16
    | UTF16LE | UTF32 | UTF8 | UTF8MB3 | UTF8MB4
    ;

intervalTypeBase
    : QUARTER | MONTH | DAY | HOUR
    | MINUTE | WEEK | SECOND | MICROSECOND
    ;

keywordsCanBeId
    : ACCOUNT | ACTION | ADMIN | AFTER | AGGREGATE | ALGORITHM | ANY
    | AT | AUDIT_ADMIN | AUTHORS | AUTOCOMMIT | AUTOEXTEND_SIZE
    | AUTO_INCREMENT | AVG | AVG_ROW_LENGTH | BACKUP_ADMIN | BEGIN | BINLOG | BINLOG_ADMIN | BINLOG_ENCRYPTION_ADMIN | BIT | BIT_AND | BIT_OR | BIT_XOR
    | BLOCK | BOOL | BTREE | CACHE | CASCADED | CHAIN | CHANGED
    | CHANNEL | CHECKSUM | PAGE_CHECKSUM | CATALOG_NAME | CIPHER
    | CLASS_ORIGIN | CLIENT | CLONE_ADMIN | CLOSE | CLUSTERING | COALESCE | CODE
    | COLUMNS | COLUMN_FORMAT | COLUMN_NAME | COMMENT | COMMIT | COMPACT
    | COMPLETION | COMPRESSED | COMPRESSION | CONCURRENT | CONNECT
    | CONNECTION | CONNECTION_ADMIN | CONSISTENT | CONSTRAINT_CATALOG | CONSTRAINT_NAME
    | CONSTRAINT_SCHEMA | CONTAINS | CONTEXT
    | CONTRIBUTORS | COPY | CPU | CURRENT | CURSOR_NAME
    | DATA | DATAFILE | DATABASES | DEALLOCATE
    | DEFAULT_AUTH | DEFINER | DELAY_KEY_WRITE | DES_KEY_FILE | DIRECTORY
    | DISABLE | DISCARD | DISK | DO | DUMPFILE | DUPLICATE
    | DYNAMIC | ENABLE | ENCRYPTION | ENCRYPTION_KEY_ADMIN | END | ENDS | ENGINE | ENGINE_ATTRIBUTE | ENGINES
    | ERROR | ERRORS | ESCAPE | EUR | EVEN | EVENT | EVENTS | EVERY | EXCEPT
    | EXCHANGE | EXCLUSIVE | EXIT | EXPIRE | EXPORT | EXTENDED | EXTENT_SIZE | FAST | FAULTS
    | FIELDS | FILE_BLOCK_SIZE | FILTER | FIREWALL_ADMIN | FIREWALL_USER | FIRST | FIXED | FLUSH
    | FOLLOWS | FOUND | FULL | FUNCTION | GENERAL | GLOBAL | GRANTS | GROUP | GROUP_CONCAT
    |  HANDLER | HASH | HELP | HOST | HOSTS | IDENTIFIED
    | IGNORED | IGNORE_SERVER_IDS | IMPORT | INDEX | INDEXES | INITIAL_SIZE | INNODB_REDO_LOG_ARCHIVE
    | INPLACE | INSERT_METHOD | INSTALL | INSTANCE | INSTANT | INTERNAL | INVOKER | IO
    | IO_THREAD | IPC | ISO | ISOLATION | ISSUER | JIS | JSON | KEY | KEY_BLOCK_SIZE
    | LANGUAGE | LAST | LEAVES | LESS | LEVEL | LIST | LOCAL
    | LOGFILE | LOGS | MASTER | MASTER_AUTO_POSITION
    | MASTER_CONNECT_RETRY | MASTER_DELAY
    | MASTER_HEARTBEAT_PERIOD | MASTER_HOST | MASTER_LOG_FILE
    | MASTER_LOG_POS | MASTER_PASSWORD | MASTER_PORT
    | MASTER_RETRY_COUNT | MASTER_SSL | MASTER_SSL_CA
    | MASTER_SSL_CAPATH | MASTER_SSL_CERT | MASTER_SSL_CIPHER
    | MASTER_SSL_CRL | MASTER_SSL_CRLPATH | MASTER_SSL_KEY
    | MASTER_TLS_VERSION | MASTER_USER
    | MAX_CONNECTIONS_PER_HOUR | MAX_QUERIES_PER_HOUR
    | MAX | MAX_ROWS | MAX_SIZE | MAX_UPDATES_PER_HOUR
    | MAX_USER_CONNECTIONS | MEDIUM | MEMBER | MEMORY | MERGE | MESSAGE | MESSAGE_TEXT
    | MID | MIGRATE
    | MIN | MIN_ROWS | MODE | MODIFY | MUTEX | MYSQL | MYSQL_ERRNO | NAME | NAMES
    | NCHAR | NDB_STORED_USER | NEVER | NEXT | NO | NOCOPY | NODEGROUP | NOCACHE | NONE | NOWAIT | NUMBER | ODBC | OFFLINE | OFFSET
    | OF | OJ | OLD_PASSWORD | ONE | ONLINE | ONLY | OPEN | OPTIMIZER_COSTS
    | OPTIONAL | OPTIONS | ORDER | OWNER | PACK_KEYS | PAGE | PARSER | PARTIAL
    | PARTITIONING | PARTITIONS | PASSWORD | PERSIST_RO_VARIABLES_ADMIN | PHASE | PLUGINS
    | PLUGIN_DIR | PLUGIN | PORT | PRECEDES | PREPARE | PRESERVE | PREV
    | PROCESSLIST | PROFILE | PROFILES | PROXY | QUERY | QUICK
    | REBUILD | RECOVER | REDO_BUFFER_SIZE | REDUNDANT
    | RELAY | RELAYLOG | RELAY_LOG_FILE | RELAY_LOG_POS | REMOVE
    | REORGANIZE | REPAIR 
    | RESET
    | RESOURCE_GROUP_ADMIN | RESOURCE_GROUP_USER | RESUME
    | RETURNED_SQLSTATE | RETURNS | ROLE | ROLE_ADMIN | ROLLBACK | ROLLUP | ROTATE | ROW | ROWS
    | ROW_FORMAT | RTREE | SAVEPOINT | SCHEDULE | SCHEMA | SCHEMAS | SCHEMA_NAME | SECURITY | SECONDARY_ENGINE_ATTRIBUTE | SERIAL | SERVER
    | SESSION | SESSION_VARIABLES_ADMIN | SET_USER_ID | SHARE | SHARED | SHOW_ROUTINE | SIGNED | SIMPLE | SLAVE
    | SLOW | SNAPSHOT | SOCKET | SOME | SONAME | SOUNDS | SOURCE
    | SQL_AFTER_GTIDS | SQL_AFTER_MTS_GAPS | SQL_BEFORE_GTIDS
    | SQL_BUFFER_RESULT | SQL_THREAD
    | STACKED | START | STARTS | STATS_AUTO_RECALC | STATS_PERSISTENT
    | STATS_SAMPLE_PAGES | STATUS | STD | STDDEV | STDDEV_POP | STDDEV_SAMP | STOP | STORAGE
    | SUBCLASS_ORIGIN | SUBJECT | SUBPARTITION | SUBPARTITIONS | SUM | SUSPEND | SWAPS
    | SWITCHES | SYSTEM_VARIABLES_ADMIN | TABLE_NAME | TABLESPACE | TABLE_ENCRYPTION_ADMIN
    | TEXT | TEMPORARY | TEMPTABLE | THAN | TRADITIONAL
    | TRANSACTION | TRANSACTIONAL | TRIGGERS | TRUNCATE | UNDEFINED | UNDOFILE
    | UNDO_BUFFER_SIZE | UNINSTALL | UNKNOWN | UNTIL | UPGRADE | USA | USER | USE_FRM | USER_RESOURCES
    | VALIDATION | VALUE | VALUES | VAR_POP | VAR_SAMP | VARIABLES | VARIANCE | VERSION_TOKEN_ADMIN | VIEW | WAIT | WARNINGS | WITHOUT
    | WRAPPER | X509 | XA | XA_RECOVER_ADMIN | XML
    ;

functionNameBase
    : ABS | ACOS | ADDDATE | ADDTIME | AES_DECRYPT | AES_ENCRYPT
    | AREA | ASBINARY | ASIN | ASTEXT | ASWKB | ASWKT
    | ASYMMETRIC_DECRYPT | ASYMMETRIC_DERIVE
    | ASYMMETRIC_ENCRYPT | ASYMMETRIC_SIGN | ASYMMETRIC_VERIFY
    | ATAN | ATAN2 | BENCHMARK | BIN | BIT_COUNT | BIT_LENGTH | BITMAP_BIT_POSITION | BITMAP_BUCKET_OFFSET | BITMAP_BUCKET_NUMBER
    | BUFFER | CEIL | CEILING | CENTROID | CHARACTER_LENGTH
    | CHARSET | CHAR_LENGTH | COERCIBILITY | COLLATION
    | COMPRESS | COALESCE | CONCAT | CONCAT_WS | CONNECTION_ID | CONV
    | CONVERT_TZ | COS | COT | CRC32
    | CREATE_ASYMMETRIC_PRIV_KEY | CREATE_ASYMMETRIC_PUB_KEY
    | CREATE_DH_PARAMETERS | CREATE_DIGEST | CROSSES | CUME_DIST | DATABASE | DATE
    | DATEDIFF | DATE_FORMAT | DAY | DAYNAME | DAYOFMONTH
    | DAYOFWEEK | DAYOFYEAR | DECODE | DEGREES | DENSE_RANK | DES_DECRYPT
    | DES_ENCRYPT | DIMENSION | DISJOINT | ELT | ENCODE
    | ENCRYPT | ENDPOINT | ENVELOPE | EQUALS | EXP | EXPORT_SET
    | EXTERIORRING | EXTRACTVALUE | FIELD | FIND_IN_SET | FIRST_VALUE | FLOOR
    | FORMAT | FOUND_ROWS | FROM_BASE64 | FROM_DAYS
    | FROM_UNIXTIME | GEOMCOLLFROMTEXT | GEOMCOLLFROMWKB
    | GEOMETRYCOLLECTION | GEOMETRYCOLLECTIONFROMTEXT
    | GEOMETRYCOLLECTIONFROMWKB | GEOMETRYFROMTEXT
    | GEOMETRYFROMWKB | GEOMETRYN | GEOMETRYTYPE | GEOMFROMTEXT
    | GEOMFROMWKB | GET_FORMAT | GET_LOCK | GLENGTH | GREATEST
    | GTID_SUBSET | GTID_SUBTRACT | HEX | HOUR | IFNULL
    | INET6_ATON | INET6_NTOA | INET_ATON | INET_NTOA | INSTR
    | INTERIORRINGN | INTERSECTS | INVISIBLE
    | ISCLOSED | ISEMPTY | ISNULL
    | ISSIMPLE | IS_FREE_LOCK | IS_IPV4 | IS_IPV4_COMPAT
    | IS_IPV4_MAPPED | IS_IPV6 | IS_USED_LOCK | LAG | LAST_INSERT_ID | LAST_VALUE
    | LCASE | LEAD | LEAST | LEFT | LENGTH | LINEFROMTEXT | LINEFROMWKB
    | LINESTRING | LINESTRINGFROMTEXT | LINESTRINGFROMWKB | LN
    | LOAD_FILE | LOCATE | LOG | LOG10 | LOG2 | LOWER | LPAD
    | LTRIM | MAKEDATE | MAKETIME | MAKE_SET | MASTER_POS_WAIT
    | MBRCONTAINS | MBRDISJOINT | MBREQUAL | MBRINTERSECTS
    | MBROVERLAPS | MBRTOUCHES | MBRWITHIN | MD5 | MICROSECOND
    | MINUTE | MLINEFROMTEXT | MLINEFROMWKB | MOD| MONTH | MONTHNAME
    | MPOINTFROMTEXT | MPOINTFROMWKB | MPOLYFROMTEXT
    | MPOLYFROMWKB | MULTILINESTRING | MULTILINESTRINGFROMTEXT
    | MULTILINESTRINGFROMWKB | MULTIPOINT | MULTIPOINTFROMTEXT
    | MULTIPOINTFROMWKB | MULTIPOLYGON | MULTIPOLYGONFROMTEXT
    | MULTIPOLYGONFROMWKB | NAME_CONST | NTH_VALUE | NTILE | NULLIF | NUMGEOMETRIES
    | NUMINTERIORRINGS | NUMPOINTS | OCT | OCTET_LENGTH | ORD
    | OVERLAPS | PERCENT_RANK | PERIOD_ADD | PERIOD_DIFF | PI | POINT
    | POINTFROMTEXT | POINTFROMWKB | POINTN | POLYFROMTEXT
    | POLYFROMWKB | POLYGON | POLYGONFROMTEXT | POLYGONFROMWKB
    | POSITION | POW | POWER | QUARTER | QUOTE | RADIANS | RAND | RANK
    | RANDOM_BYTES | RELEASE_LOCK | REVERSE | RIGHT | ROUND
    | ROW_COUNT | ROW_NUMBER | RPAD | RTRIM | SECOND | SEC_TO_TIME
    | SCHEMA | SESSION_USER | SESSION_VARIABLES_ADMIN
    | SHA | SHA1 | SHA2 | SIGN | SIN | SLEEP
    | SOUNDEX | SQL_THREAD_WAIT_AFTER_GTIDS | SQRT | SRID
    | STARTPOINT | STRCMP | STR_TO_DATE | ST_AREA | ST_ASBINARY
    | ST_ASTEXT | ST_ASWKB | ST_ASWKT | ST_BUFFER | ST_CENTROID
    | ST_CONTAINS | ST_CROSSES | ST_DIFFERENCE | ST_DIMENSION
    | ST_DISJOINT | ST_DISTANCE | ST_ENDPOINT | ST_ENVELOPE
    | ST_EQUALS | ST_EXTERIORRING | ST_GEOMCOLLFROMTEXT
    | ST_GEOMCOLLFROMTXT | ST_GEOMCOLLFROMWKB
    | ST_GEOMETRYCOLLECTIONFROMTEXT
    | ST_GEOMETRYCOLLECTIONFROMWKB | ST_GEOMETRYFROMTEXT
    | ST_GEOMETRYFROMWKB | ST_GEOMETRYN | ST_GEOMETRYTYPE
    | ST_GEOMFROMTEXT | ST_GEOMFROMWKB | ST_INTERIORRINGN
    | ST_INTERSECTION | ST_INTERSECTS | ST_ISCLOSED | ST_ISEMPTY
    | ST_ISSIMPLE | ST_LINEFROMTEXT | ST_LINEFROMWKB
    | ST_LINESTRINGFROMTEXT | ST_LINESTRINGFROMWKB
    | ST_NUMGEOMETRIES | ST_NUMINTERIORRING
    | ST_NUMINTERIORRINGS | ST_NUMPOINTS | ST_OVERLAPS
    | ST_POINTFROMTEXT | ST_POINTFROMWKB | ST_POINTN
    | ST_POLYFROMTEXT | ST_POLYFROMWKB | ST_POLYGONFROMTEXT
    | ST_POLYGONFROMWKB | ST_SRID | ST_STARTPOINT
    | ST_SYMDIFFERENCE | ST_TOUCHES | ST_UNION | ST_WITHIN
    | ST_X | ST_Y | SUBDATE | SUBSTRING_INDEX | SUBTIME
    | SYSTEM_USER | TAN | TIME | TIMEDIFF | TIMESTAMP
    | TIMESTAMPADD | TIMESTAMPDIFF | TIME_FORMAT | TIME_TO_SEC
    | TOUCHES | TO_BASE64 | TO_DAYS | TO_SECONDS | UCASE
    | UNCOMPRESS | UNCOMPRESSED_LENGTH | UNHEX | UNIX_TIMESTAMP
    | UPDATEXML | UPPER | UUID | UUID_SHORT
    | VALIDATE_PASSWORD_STRENGTH | VERSION | VISIBLE
    | WAIT_UNTIL_SQL_THREAD_AFTER_GTIDS | WEEK | WEEKDAY
    | WEEKOFYEAR | WEIGHT_STRING | WITHIN | YEAR | YEARWEEK
    | Y_FUNCTION | X_FUNCTION
    | JSON_ARRAY | JSON_OBJECT | JSON_QUOTE | JSON_CONTAINS | JSON_CONTAINS_PATH
    | JSON_EXTRACT | JSON_KEYS | JSON_OVERLAPS | JSON_SEARCH | JSON_VALUE
    | JSON_ARRAY_APPEND | JSON_ARRAY_INSERT | JSON_INSERT | JSON_MERGE
    | JSON_MERGE_PATCH | JSON_MERGE_PRESERVE | JSON_REMOVE | JSON_REPLACE
    | JSON_SET | JSON_UNQUOTE | JSON_DEPTH | JSON_LENGTH | JSON_TYPE
    | JSON_VALID | JSON_TABLE | JSON_SCHEMA_VALID | JSON_SCHEMA_VALIDATION_REPORT
    | JSON_PRETTY | JSON_STORAGE_FREE | JSON_STORAGE_SIZE | JSON_ARRAYAGG
    | JSON_OBJECTAGG
    ;
