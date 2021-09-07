parser grammar NorseParser;

options { tokenVocab=NorseLexer; }

pipe
    : expression                                                          # NormalExpression
    | pipe BITOR methodCallExpression                                     # PipeCall;

expression
    : primaryExpression                                                   # Primary
    | expression DOT IDENTIFIER                                           # Field
    | methodCallExpression                                                # MethodCall
    | prefix=(ADD|SUB) expression                                         # UnarySign
    | prefix=(TILDE|BANG) expression                                      # UnaryTildeBang
    | expression bop=(MUL|DIV|MOD) expression                             # Multiplicative
    | expression bop=(ADD|SUB) expression                                 # Additive
    | expression bop=(LE | GE | GT | LT) expression                       # RelOps
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
    : LPAREN pipe RPAREN
    | recordConstructor
    | arrayConstructor
    | tupleConstructor
    | literal
    | UNDERBAR
    | IDENTIFIER
    ;

recordConstructor
    : LBRACE RBRACE
    | LBRACE keyValueMapping (COMMA keyValueMapping)* RBRACE
    ;

keyValueMapping
    : IDENTIFIER ARROW pipe
    ;

arrayConstructor
    : LBRACK RBRACK
    | LBRACK pipe (COMMA pipe)* RBRACK
    ;

tupleConstructor
    : LPAREN pipe (COMMA pipe)+ RPAREN
    ;

literal
    : integerLiteral
    | floatLiteral
    | CHAR_LITERAL
    | STRING_LITERAL
    | BOOL_LITERAL
    | NULL_LITERAL
    ;

integerLiteral
    : DECIMAL_LITERAL
    | HEX_LITERAL
    | OCT_LITERAL
    | BINARY_LITERAL
    ;

floatLiteral
    : FLOAT_LITERAL
    | HEX_FLOAT_LITERAL
    ;
