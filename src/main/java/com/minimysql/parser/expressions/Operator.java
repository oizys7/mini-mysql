package com.minimysql.parser.expressions;

/**
 * Operator - 二元运算符
 *
 * 定义SQL中支持的二元运算符。
 */
public enum Operator {

    /** 等于 */
    EQUAL("=", 2),
    /** 不等于 */
    NOT_EQUAL("!=", 2),
    /** 大于 */
    GREATER_THAN(">", 2),
    /** 小于 */
    LESS_THAN("<", 2),
    /** 大于等于 */
    GREATER_EQUAL(">=", 2),
    /** 小于等于 */
    LESS_EQUAL("<=", 2),
    /** 加法 */
    ADD("+", 3),
    /** 减法 */
    SUBTRACT("-", 3),
    /** 乘法 */
    MULTIPLY("*", 3),
    /** 除法 */
    DIVIDE("/", 3),
    /** 取模 */
    MODULO("%", 3),
    /** 逻辑与 */
    AND("AND", 1),
    /** 逻辑或 */
    OR("OR", 0);

    /** 运算符字符串表示 */
    private final String symbol;

    /** 优先级(数值越大优先级越高) */
    private final int precedence;

    Operator(String symbol, int precedence) {
        this.symbol = symbol;
        this.precedence = precedence;
    }

    public String getSymbol() {
        return symbol;
    }

    public int getPrecedence() {
        return precedence;
    }

    /**
     * 根据符号获取运算符
     *
     * @param symbol 运算符符号
     * @return 运算符枚举,如果未知返回null
     */
    public static Operator fromSymbol(String symbol) {
        for (Operator op : values()) {
            if (op.symbol.equals(symbol)) {
                return op;
            }
        }
        return null;
    }

    @Override
    public String toString() {
        return symbol;
    }
}
