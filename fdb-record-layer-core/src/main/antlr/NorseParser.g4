parser grammar NorseParser;

options { tokenVocab=NorseLexer; }

pipe
    : expression                                                          # NormalExpression
    | pipe BITOR methodCallExpression                                     # PipeCall;

expression
    : primaryExpression                                                   # Primary
    | expression DOT TUPLE_FIELD                                          # TupleField
    | expression DOT IDENTIFIER                                           # Field
    | methodCallExpression                                                # MethodCall
    | prefix=(ADD|SUB) expression                                         # UnarySign
    | prefix=(TILDE|BANG) expression                                      # UnaryTildeBang
    | expression bop=(MUL | DIV | MOD) expression                         # Multiplicative
    | expression bop=(ADD | SUB) expression                               # Additive
    | expression bop=(LE | GE | GT | LT) expression                       # Inequality
    | expression bop=(EQUAL | NOTEQUAL) expression                        # EqualityNonEquality
    | expression bop=AND expression                                       # LogicalAnd
    | expression bop=OR expression                                        # LogicalOr
    | <assoc=right> expression bop=QUESTION expression COLON expression   # FunctionalIf
    | <assoc=right> expression bop=COLONEQUALS expression                 # Assign
    | lambdaExpression                                                    # Lambda
    | comprehensionExpression                                             # Comprehension
    ;

expressionList
    : expression (COMMA expression)*
    ;

methodCallExpression
    : IDENTIFIER LPAREN expressionList? RPAREN
    | IDENTIFIER expression
    ;

lambdaExpression
    : lambdaParameters DOUBLE_ARROW expression
    ;

lambdaParameters
    : IDENTIFIER
    | LPAREN IDENTIFIER (COMMA IDENTIFIER)* RPAREN
    ;

comprehensionExpression
    : LBRACK comprehensionBinding (SEMI comprehensionBinding)* RBRACK
    ;

comprehensionBinding
    : IDENTIFIER BACK_ARROW pipe
    | IDENTIFIER COLONEQUALS pipe
    ;

primaryExpression
    : LPAREN pipe RPAREN   # PrimaryExpressionFromNestedPipe
    | recordConstructor    # PrimaryExpressionFromRecordConstructor
    | literal              # PrimaryExpressionFromLiteral
    | UNDERBAR             # PrimaryExpressionFromUnderbar
    | IDENTIFIER           # PrimaryExpressionFromIdentifier
    ;

recordConstructor
    : LBRACE RBRACE
    | LBRACE keyValueMapping (COMMA keyValueMapping)* RBRACE
    ;

keyValueMapping
    : IDENTIFIER ARROW pipe
    ;

literal
    : integerLiteralLong
    | integerLiteral
    | floatLiteralDouble
    | floatLiteral
    | STRING_LITERAL
    | BOOL_LITERAL
    | NULL_LITERAL
    ;

integerLiteralLong
    : DECIMAL_LITERAL_LONG
    | HEX_LITERAL_LONG
    | OCT_LITERAL_LONG
    | BINARY_LITERAL_LONG
    ;

integerLiteral
    : DECIMAL_LITERAL
    | HEX_LITERAL
    | OCT_LITERAL
    | BINARY_LITERAL
    ;

floatLiteralDouble
    : FLOAT_LITERAL_DOUBLE
    | HEX_FLOAT_LITERAL_DOUBLE
    ;

floatLiteral
    : FLOAT_LITERAL
    | HEX_FLOAT_LITERAL
    ;
