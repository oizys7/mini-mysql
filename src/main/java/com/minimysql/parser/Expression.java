package com.minimysql.parser;

/**
 * Expression - SQL表达式接口
 *
 * 表示SQL中的各种表达式,包括:
 * - 列引用: id, name, users.age
 * - 字面量: 42, 'hello', TRUE, NULL
 * - 二元运算: age > 18, name = 'John'
 * - 逻辑运算: age > 18 AND age < 65
 *
 * 设计原则:
 * - "Good taste": 所有表达式都是Expression,消除if-else判断
 * - 类型安全: 每种表达式有专门的子类
 * - 可组合: 复杂表达式由简单表达式组合而成
 *
 * 使用示例:
 * <pre>
 * Expression expr = new BinaryExpression(
 *     new ColumnExpression("age"),
 *     Operator.GT,
 *     new LiteralExpression(18)
 * );
 * </pre>
 */
public interface Expression {

    /**
     * 获取表达式类型
     *
     * @return 表达式类型枚举
     */
    ExpressionType getType();

    /**
     * SQL表达式类型枚举
     */
    enum ExpressionType {
        /** 列引用 */
        COLUMN,
        /** 字面量 */
        LITERAL,
        /** 二元运算 */
        BINARY,
        /** NOT运算 */
        NOT
    }
}
