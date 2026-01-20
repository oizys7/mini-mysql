package com.minimysql.parser.statements;

import com.minimysql.parser.Expression;
import com.minimysql.parser.Statement;

import java.util.Optional;

/**
 * DeleteStatement - DELETE删除语句
 *
 * 表示删除数据的SQL语句。
 *
 * 语法示例:
 * <pre>
 * DELETE FROM users WHERE id = 1;
 * DELETE FROM users WHERE age > 100;
 * </pre>
 *
 * 设计原则:
 * - 支持WHERE条件(可选,但没有WHERE会删除所有行!)
 * - 最简单的DML语句之一
 */
public class DeleteStatement implements Statement {

    /** 表名 */
    private final String tableName;

    /** WHERE条件(如果没有WHERE子句则为空) */
    private final Expression whereClause;

    public DeleteStatement(String tableName, Expression whereClause) {
        this.tableName = tableName;
        this.whereClause = whereClause;
    }

    public String getTableName() {
        return tableName;
    }

    public Optional<Expression> getWhereClause() {
        return Optional.ofNullable(whereClause);
    }

    @Override
    public StatementType getType() {
        return StatementType.DELETE;
    }

    @Override
    public String toString() {
        return "DeleteStatement{" +
                "tableName='" + tableName + '\'' +
                ", whereClause=" + whereClause +
                '}';
    }
}
