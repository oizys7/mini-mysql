package com.minimysql.parser.expressions;

import com.minimysql.parser.Expression;

/**
 * NotExpression - NOT运算表达式
 *
 * 表示逻辑NOT运算,如:
 * - NOT (age > 18)
 * - NOT active
 *
 * 设计原则:
 * - 不可变对象
 * - 简单的一元运算符
 */
public class NotExpression implements Expression {

    /** 操作数 */
    private final Expression operand;

    public NotExpression(Expression operand) {
        this.operand = operand;
    }

    public Expression getOperand() {
        return operand;
    }

    @Override
    public ExpressionType getType() {
        return ExpressionType.NOT;
    }

    @Override
    public String toString() {
        return "(NOT " + operand + ")";
    }
}
