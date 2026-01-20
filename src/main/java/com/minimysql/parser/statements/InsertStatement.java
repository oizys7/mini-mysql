package com.minimysql.parser.statements;

import com.minimysql.parser.Expression;
import com.minimysql.parser.Statement;

import java.util.List;

/**
 * InsertStatement - INSERT插入语句
 *
 * 表示插入数据的SQL语句,支持单行和多行插入。
 *
 * 语法示例:
 * <pre>
 * INSERT INTO users VALUES (1, 'Alice', 25);
 * INSERT INTO users (id, name, age) VALUES (1, 'Alice', 25);
 * INSERT INTO users VALUES (1, 'Alice', 25), (2, 'Bob', 30);
 * </pre>
 *
 * 设计原则:
 * - 支持指定列名(可选)
 * - 支持批量插入(多行VALUES)
 * - 每行是一个表达式列表
 */
public class InsertStatement implements Statement {

    /** 表名 */
    private final String tableName;

    /** 列名列表(为空表示按表定义顺序插入) */
    private final List<String> columnNames;

    /** 值列表(每个元素代表一行,每行是一个表达式列表) */
    private final List<List<Expression>> values;

    public InsertStatement(String tableName,
                          List<String> columnNames,
                          List<List<Expression>> values) {
        this.tableName = tableName;
        this.columnNames = columnNames != null ? List.copyOf(columnNames) : List.of();
        this.values = List.copyOf(values);
    }

    public String getTableName() {
        return tableName;
    }

    public List<String> getColumnNames() {
        return columnNames;
    }

    /**
     * 判断是否指定了列名
     */
    public boolean hasColumnNames() {
        return !columnNames.isEmpty();
    }

    public List<List<Expression>> getValues() {
        return values;
    }

    /**
     * 获取插入的行数
     */
    public int getRowCount() {
        return values.size();
    }

    @Override
    public StatementType getType() {
        return StatementType.INSERT;
    }

    @Override
    public String toString() {
        return "InsertStatement{" +
                "tableName='" + tableName + '\'' +
                ", columnNames=" + columnNames +
                ", rowCount=" + getRowCount() +
                '}';
    }
}
