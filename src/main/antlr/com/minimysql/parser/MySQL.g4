grammar MySQL;

// ==================== 关键字 ====================
CREATE:      'CREATE';
TABLE:       'TABLE';
DROP:        'DROP';
SELECT:      'SELECT';
INSERT:      'INSERT';
INTO:        'INTO';
VALUES:      'VALUES';
UPDATE:      'UPDATE';
SET:         'SET';
DELETE:      'DELETE';
FROM:        'FROM';
WHERE:       'WHERE';
AND:         'AND';
OR:          'OR';
NOT:         'NOT';
NULL_:       'NULL';
PRIMARY:     'PRIMARY';
KEY:         'KEY';

// 数据类型
INT:         'INT';
BIGINT:      'BIGINT';
DOUBLE:      'DOUBLE';
BOOLEAN:     'BOOLEAN';
VARCHAR:     'VARCHAR';
DATE:        'DATE';
TIMESTAMP:   'TIMESTAMP';

// 运算符
ASSIGN:      '=';
GT:          '>';
LT:          '<';
GTE:         '>=';
LTE:         '<=';
NEQ:         '!=' | '<>';
PLUS:        '+';
MINUS:       '-';
STAR:        '*';
DIVIDE:      '/';
MOD:         '%';

// 分隔符
LPAREN:      '(';
RPAREN:      ')';
COMMA:       ',';
SEMICOLON:   ';';
DOT:         '.';

// 标识符
IDENTIFIER:
    [a-zA-Z_][a-zA-Z0-9_]*
;

// 字面量
INTEGER_LITERAL:
    [0-9]+
;

DECIMAL_LITERAL:
    [0-9]+ '.' [0-9]*
    | '.' [0-9]+
;

STRING_LITERAL:
    '\'' ( ~'\'' | '\'\'' )* '\''
;

BOOLEAN_LITERAL:
    'TRUE' | 'FALSE'
;

// 空白符和注释
WS:
    [ \t\r\n\u000C]+ -> skip
;

COMMENT:
    '--' ~[\r\n]* -> skip
;

MULTILINE_COMMENT:
    '/*' .*? '*/' -> skip
;

// ==================== PARSER RULES ====================

// ==================== 顶层规则 ====================
sqlStatement:
    createTableStatement
    | dropTableStatement
    | selectStatement
    | insertStatement
    | updateStatement
    | deleteStatement
    | SEMICOLON
;

// ==================== CREATE TABLE ====================
createTableStatement:
    CREATE TABLE tableName=identifier
    LPAREN columnDefinition (COMMA columnDefinition)* (COMMA PRIMARY KEY LPAREN columnName=identifier RPAREN)? RPAREN
    SEMICOLON?
;

columnDefinition:
    columnName=identifier dataType (NOT NULL_)?
;

dataType:
    INT
    | BIGINT
    | DOUBLE
    | BOOLEAN
    | VARCHAR LPAREN length=INTEGER_LITERAL RPAREN
    | DATE
    | TIMESTAMP
;

// ==================== DROP TABLE ====================
dropTableStatement:
    DROP TABLE tableName=identifier SEMICOLON?
;

// ==================== SELECT ====================
selectStatement:
    SELECT selectItems (FROM tableName=identifier) (WHERE whereExpr=expression)? SEMICOLON?
;

selectItems:
    STAR
    | expression (COMMA expression)*
;

// ==================== INSERT ====================
insertStatement:
    INSERT INTO tableName=identifier
    (LPAREN identifier (COMMA identifier)* RPAREN)?
    VALUES insertStatementValueRow (COMMA insertStatementValueRow)*
    SEMICOLON?
;

insertStatementValueRow:
    LPAREN expression (COMMA expression)* RPAREN
;

// ==================== UPDATE ====================
updateStatement:
    UPDATE tableName=identifier
    SET setItem (COMMA setItem)*
    (WHERE whereExpr=expression)?
    SEMICOLON?
;

setItem:
    columnName=identifier ASSIGN valueExpr=expression
;

// ==================== DELETE ====================
deleteStatement:
    DELETE FROM tableName=identifier
    (WHERE whereExpr=expression)?
    SEMICOLON?
;

// ==================== 表达式 ====================
expression:
    LPAREN expression RPAREN                          # parenthesisExpr
    | expression op=(STAR | DIVIDE | MOD) expression # arithmeticExpr
    | expression op=(PLUS | MINUS) expression        # arithmeticExpr
    | expression op=(GT | LT | GTE | LTE | NEQ | ASSIGN) expression # comparisonExpr
    | NOT expression                                  # notExpr
    | expression op=AND expression                       # logicalExpr
    | expression op=OR expression                        # logicalExpr
    | literal                                         # literalExpr
    | identifier (DOT identifier)*                   # columnExpr
;

// ==================== 字面量 ====================
literal:
    INTEGER_LITERAL
    | DECIMAL_LITERAL
    | STRING_LITERAL
    | BOOLEAN_LITERAL
    | NULL_
;

// ==================== 标识符 ====================
identifier:
    IDENTIFIER
;
