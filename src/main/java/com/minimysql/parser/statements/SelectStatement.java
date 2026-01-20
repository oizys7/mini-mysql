package com.minimysql.parser.statements;

import com.minimysql.parser.Expression;
import com.minimysql.parser.Statement;

import java.util.List;
import java.util.Optional;

/**
 * SelectStatement - SELECT查询语句
 *
 * 表示查询数据的SQL语句。
 *
 * 语法示例:
 * <pre>
 * SELECT * FROM users;
 * SELECT id, name FROM users WHERE age > 18;
 * </pre>
 *
 * 设计原则:
 * - 支持SELECT列表(可以是*或具体列)
 * - 支持WHERE条件(可选)
 * - 不支持JOIN、ORDER BY、GROUP BY等高级特性(保持简单)
 */
public class SelectStatement implements Statement {

    /** SELECT列表(为空表示SELECT *) */
    private final List<Expression> selectItems;

    /** 表名 */
    private final String tableName;

    /** WHERE条件(如果没有WHERE子句则为空) */
    private final Expression whereClause;

    public SelectStatement(List<Expression> selectItems, String tableName, Expression whereClause) {
        this.selectItems = selectItems != null ? List.copyOf(selectItems) : List.of();
        this.tableName = tableName;
        this.whereClause = whereClause;
    }

    /**
     * 判断是否为SELECT *
     */
    public boolean isSelectAll() {
        return selectItems.isEmpty();
    }

    public List<Expression> getSelectItems() {
        return selectItems;
    }

    public String getTableName() {
        return tableName;
    }

    public Optional<Expression> getWhereClause() {
        return Optional.ofNullable(whereClause);
    }

    @Override
    public StatementType getType() {
        return StatementType.SELECT;
    }

    @Override
    public String toString() {
        return "SelectStatement{" +
                "selectItems=" + (isSelectAll() ? "*" : selectItems) +
                ", tableName='" + tableName + '\'' +
                ", whereClause=" + whereClause +
                '}';
    }
}
