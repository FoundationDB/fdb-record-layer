parser grammar NorseParser;

options { tokenVocab=NorseLexer; }

pipe
    : expression                                                          # PipeExpression
    | pipe BITOR methodCall                                               # PipeMethodCall
    ;

expression
    : primaryExpression                                                   # ExpressionPrimaryExpression
    | expression HASHTAG DECIMAL_LITERAL                                  # ExpressionTupleField
    | expression DOT IDENTIFIER                                           # ExpressionField
    | functionCall                                                        # ExpressionFunctionCall
    | prefix=(ADD|SUB) expression                                         # ExpressionUnarySign
    | prefix=BANG expression                                              # ExpressionUnaryBang
    | expression bop=(MUL | DIV | MOD) expression                         # ExpressionMultiplicative
    | expression bop=(ADD | SUB) expression                               # ExpressionAdditive
    | expression bop=(LE | GE | GT | LT) expression                       # ExpressionInequality
    | expression bop=(EQUAL | NOTEQUAL) expression                        # ExpressionEqualityNonEquality
    | expression bop=AND expression                                       # ExpressionLogicalAnd
    | expression bop=OR expression                                        # ExpressionLogicalOr
    | <assoc=right> expression bop=QUESTION expression COLON expression   # ExpressionFunctionalIf
    | <assoc=right> expression bop=COLONEQUALS expression                 # ExpressionAssign
    | lambda                                                              # ExpressionLambda
    | comprehension                                                       # ExpressionComprehension
    ;

functionCall
    : methodCall
    ;

methodCall
    : IDENTIFIER arguments
    ;

arguments
    : tuple                      # ArgumentsPipes
    | expression                 # ArgumentsExpression
    | LPAREN RPAREN              # ArgumentsExmpty
    ;

tuple
    : LPAREN pipe (COMMA pipe)* RPAREN                      # TuplePipes
    ;

lambda
    : extractor DOUBLE_ARROW expression
    ;

extractor
    : bindingIdentifier
    | LPAREN bindingIdentifier (COMMA bindingIdentifier)* RPAREN
    ;

bindingIdentifier
    : IDENTIFIER
    | UNDERBAR
    ;

comprehension
    : LBRACK tuple COLON comprehensionBindings RBRACK   # ComprehensionWithBindings
    | LBRACK pipe RBRACK                                # ComprehensionSimple
    ;

comprehensionBindings
    : comprehensionBinding (SEMI comprehensionBinding)*
    ;

comprehensionBinding
    : extractor BACK_ARROW pipe              # ComprehensionBindingIteration
    | IDENTIFIER COLONEQUALS pipe            # ComprehensionBindingAssign
    | IF pipe                                # ComprehensionBindingIf
    ;

primaryExpression
    : LPAREN pipe RPAREN                           # PrimaryExpressionNestedPipe
    | tuple                                        # PrimaryExpressionTuple
    | recordConstructor                            # PrimaryExpressionFromRecordConstructor
    | literal                                      # PrimaryExpressionFromLiteral
    | UNDERBAR                                     # PrimaryExpressionFromUnderbar
    | IDENTIFIER                                   # PrimaryExpressionFromIdentifier
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
