package com.minimysql.parser.expressions;

import com.minimysql.parser.Expression;

import java.util.List;
import java.util.Collections;

/**
 * ColumnExpression - 列引用表达式
 *
 * 表示对表中列的引用,支持带表名前缀:
 * - 简单列引用: id, name, age
 * - 带表名前缀: users.id, orders.customer_id
 *
 * 设计原则:
 * - 不可变对象
 * - 统一表示: 多级列名用List存储,消除特殊情况
 */
public class ColumnExpression implements Expression {

    /** 列名(可能包含表名前缀,如["users", "id"]) */
    private final List<String> columnNameParts;

    public ColumnExpression(String columnName) {
        this.columnNameParts = List.of(columnName);
    }

    public ColumnExpression(List<String> columnNameParts) {
        this.columnNameParts = List.copyOf(columnNameParts);
    }

    /**
     * 获取列名(不含表名前缀)
     *
     * @return 列名
     */
    public String getColumnName() {
        return columnNameParts.get(columnNameParts.size() - 1);
    }

    /**
     * 获取表名(如果有)
     *
     * @return 表名,如果没有表名前缀则返回null
     */
    public String getTableName() {
        return columnNameParts.size() > 1 ? columnNameParts.get(0) : null;
    }

    /**
     * 获取完整的列名(包含表名前缀)
     *
     * @return 完整列名,如"users.id"
     */
    public String getFullName() {
        return String.join(".", columnNameParts);
    }

    public List<String> getColumnNameParts() {
        return Collections.unmodifiableList(columnNameParts);
    }

    @Override
    public ExpressionType getType() {
        return ExpressionType.COLUMN;
    }

    @Override
    public String toString() {
        return getFullName();
    }
}
