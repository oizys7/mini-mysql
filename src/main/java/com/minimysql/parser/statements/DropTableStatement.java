package com.minimysql.parser.statements;

import com.minimysql.parser.Statement;

/**
 * DropTableStatement - DROP TABLE语句
 *
 * 表示删除表的SQL语句。
 *
 * 语法示例:
 * <pre>
 * DROP TABLE users;
 * </pre>
 *
 * 设计原则:
 * - 最简单的Statement之一,只包含表名
 * - "Good taste": 没有CASCADE/RESTRICT等特殊情况
 */
public class DropTableStatement implements Statement {

    /** 表名 */
    private final String tableName;

    public DropTableStatement(String tableName) {
        this.tableName = tableName;
    }

    public String getTableName() {
        return tableName;
    }

    @Override
    public StatementType getType() {
        return StatementType.DROP_TABLE;
    }

    @Override
    public String toString() {
        return "DropTableStatement{" +
                "tableName='" + tableName + '\'' +
                '}';
    }
}
