package com.minimysql.parser.expressions;

import com.minimysql.parser.Expression;

/**
 * BinaryExpression - 二元运算表达式
 *
 * 表示需要两个操作数的运算,包括:
 * - 算术运算: age + 1, price * quantity
 * - 比较运算: age > 18, name = 'John'
 * - 逻辑运算: age > 18 AND age < 65
 *
 * 设计原则:
 * - 不可变对象
 * - 左操作数、运算符、右操作数
 * - 支持嵌套: (age > 18) AND (name IS NOT NULL)
 */
public class BinaryExpression implements Expression {

    /** 左操作数 */
    private final Expression left;

    /** 运算符 */
    private final Operator operator;

    /** 右操作数 */
    private final Expression right;

    public BinaryExpression(Expression left, Operator operator, Expression right) {
        this.left = left;
        this.operator = operator;
        this.right = right;
    }

    public Expression getLeft() {
        return left;
    }

    public Operator getOperator() {
        return operator;
    }

    public Expression getRight() {
        return right;
    }

    @Override
    public ExpressionType getType() {
        return ExpressionType.BINARY;
    }

    @Override
    public String toString() {
        return "(" + left + " " + operator + " " + right + ")";
    }
}
