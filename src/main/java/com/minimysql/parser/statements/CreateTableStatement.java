package com.minimysql.parser.statements;

import com.minimysql.parser.Statement;

import java.util.List;
import java.util.Optional;

/**
 * CreateTableStatement - CREATE TABLE语句
 *
 * 表示创建表的SQL语句。
 *
 * 语法示例:
 * <pre>
 * CREATE TABLE users (
 *     id INT NOT NULL,
 *     name VARCHAR(100),
 *     PRIMARY KEY (id)
 * );
 * </pre>
 *
 * 设计原则:
 * - 包含表名、列定义、主键信息
 * - 主键是可选的(没有PRIMARY KEY子句则为空)
 */
public class CreateTableStatement implements Statement {

    /** 表名 */
    private final String tableName;

    /** 列定义列表 */
    private final List<ColumnDefinition> columns;

    /** 主键列名(如果没有PRIMARY KEY子句则为空) */
    private final String primaryKeyColumn;

    public CreateTableStatement(String tableName,
                                 List<ColumnDefinition> columns,
                                 String primaryKeyColumn) {
        this.tableName = tableName;
        this.columns = List.copyOf(columns); // 不可变视图
        this.primaryKeyColumn = primaryKeyColumn;
    }

    public String getTableName() {
        return tableName;
    }

    public List<ColumnDefinition> getColumns() {
        return columns;
    }

    public Optional<String> getPrimaryKeyColumn() {
        return Optional.ofNullable(primaryKeyColumn);
    }

    @Override
    public StatementType getType() {
        return StatementType.CREATE_TABLE;
    }

    @Override
    public String toString() {
        return "CreateTableStatement{" +
                "tableName='" + tableName + '\'' +
                ", columns=" + columns +
                ", primaryKeyColumn='" + primaryKeyColumn + '\'' +
                '}';
    }
}
