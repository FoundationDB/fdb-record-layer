/*
 * A (potentially temporary) lexer grammer for a DDL language. At some point (possibly soon) this is likely
 to be merged with the larger Relational Parser, but when this file was written such a parser did not exist in main.
*/

lexer grammar DdlLexer;

//action keywords
KW_CREATE: C R E A T E;
KW_AS: A S;
KW_WITH: W I T H;
KW_HAS: H A S;
KW_DROP : D R O P;
KW_SHOW: S H O W;
KW_DESCRIBE: D E S C R I B E;
KW_ON: O N;

//object keywords
KW_SCHEMA: S C H E M A;
KW_TABLE: T A B L E;
KW_INDEX: I N D E X;
KW_TEMPLATE: T E M P L A T E;
KW_DEFAULT: D E F A U L T;
KW_NULL: N U L L;
KW_PRIMARY: P R I M A R Y;
KW_KEY: K E Y;
KW_TYPE: T Y P E;
KW_DATABASE: D A T A B A S E;
KW_DATABASES: D A T A B A S E S;
KW_PREFIX: P R E F I X;
KW_TEMPLATES: T E M P L A T E S;
KW_VALUE: V A L U E;
KW_UNIQUE: U N I Q U E;
KW_INCLUDE: I N C L U D E;
KW_RECORD_TYPE: R E C O R D ' ' T Y P E;

//data type keywords
KW_BOOLEAN: B O O L E A N;
KW_BYTES: B Y T E S;
KW_DOUBLE: D O U B L E;
KW_INT64: I N T '64';
KW_STRING: S T R I N G;
KW_TIMESTAMP: T I M E S T A M P;
KW_MESSAGE: M E S S A G E;

//options keywords
KW_REPEATED: R E P E A T E D;



//letters -- this is to support case insensitive parsing
fragment A : [aA];
fragment B : [bB];
fragment C : [cC];
fragment D : [dD];
fragment E : [eE];
fragment F : [fF];
fragment G : [gG];
fragment H : [hH];
fragment I : [iI];
fragment J : [jJ];
fragment K : [kK];
fragment L : [lL];
fragment M : [mM];
fragment N : [nN];
fragment O : [oO];
fragment P : [pP];
fragment Q : [qQ];
fragment R : [rR];
fragment S : [sS];
fragment T : [tT];
fragment U : [uU];
fragment V : [vV];
fragment W : [wW];
fragment X : [xX];
fragment Y : [yY];
fragment Z : [zZ];

DOT: '.';
COLON: ':';
COMMA: ',';
SEMICOLON: ';';

LPAREN: '(';
RPAREN: ')';
LSQUARE: '[';
RSQUARE: ']';
LCURLY: '{';
RCURLY: '}';

fragment
Letter
    : 'a'..'z' | 'A'..'Z'
    ;

fragment
Digit
    : '0'..'9'
    ;

QuotedString
    :
        '\'' ('\'\'' |~('\''))* '\''
     ;

Identifier
    : (Letter | Digit)(Letter | Digit | '_'| '-')*
    | QuotedString
    ;

Path
    : '/' (Letter | Digit) (Letter | Digit | '_' | '/')* (Letter | Digit | '_')
    | QuotedString
    ;

WS: (' '|'\r'|'\t'|'\n') -> channel(HIDDEN);