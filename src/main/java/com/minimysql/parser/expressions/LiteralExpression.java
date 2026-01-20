package com.minimysql.parser.expressions;

import com.minimysql.parser.Expression;

/**
 * LiteralExpression - 字面量表达式
 *
 * 表示SQL中的字面量值,包括:
 * - 整数: 42, -100
 * - 浮点数: 3.14, -0.5
 * - 字符串: 'hello', 'it''s'
 * - 布尔值: TRUE, FALSE
 * - NULL值: NULL
 *
 * 设计原则:
 * - 不可变对象
 * - 使用Object存储值,支持多种类型
 * - NULL值用null表示
 */
public class LiteralExpression implements Expression {

    /** 字面量值(可能是Integer, Long, Double, String, Boolean, 或null) */
    private final Object value;

    public LiteralExpression(Object value) {
        this.value = value;
    }

    public Object getValue() {
        return value;
    }

    /**
     * 判断是否为NULL值
     */
    public boolean isNull() {
        return value == null;
    }

    /**
     * 获取值的类型
     */
    public Class<?> getValueType() {
        return value != null ? value.getClass() : null;
    }

    @Override
    public ExpressionType getType() {
        return ExpressionType.LITERAL;
    }

    @Override
    public String toString() {
        if (value == null) {
            return "NULL";
        }
        if (value instanceof String) {
            return "'" + value + "'";
        }
        return value.toString();
    }
}
