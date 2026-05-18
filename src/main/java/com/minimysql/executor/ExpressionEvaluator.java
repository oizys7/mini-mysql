package com.minimysql.executor;

import com.minimysql.parser.Expression;
import com.minimysql.parser.expressions.*;
import com.minimysql.storage.table.Column;
import com.minimysql.storage.table.Row;
import com.minimysql.storage.table.Table;

import java.util.List;

/**
 * ExpressionEvaluator - SQL表达式求值器
 *
 * 递归求值SQL表达式树,支持:
 * - 列引用: age → 从Row获取值
 * - 字面量: 42, 'hello' → 直接返回
 * - 二元运算: age > 18, name = 'Alice' → 比较/逻辑运算
 * - NOT运算: NOT (age > 18) → 布尔取反
 *
 * 设计原则:
 * - "Good taste": 递归求值,消除if-else判断表达式类型
 * - 类型严格: 类型不匹配直接抛异常,不自动转换
 * - 零状态: 无状态函数,纯函数式求值
 *
 * 使用示例:
 * <pre>
 * Row row = ...; // {id=1, name="Alice", age=25}
 * Expression expr = new BinaryExpression(
 *     new ColumnExpression("age"),
 *     Operator.GREATER_THAN,
 *     new LiteralExpression(18)
 * );
 *
 * Object result = evaluator.eval(expr, row);
 * // result = true (25 > 18)
 * </pre>
 *
 * MySQL对应:
 * - MySQL Executor中的Item::valXXX()方法族
 * - 表达式求值是SQL执行的核心
 */
public class ExpressionEvaluator {

    /**
     * 求值表达式(完整版本)
     *
     * @param expr 表达式
     * @param row 行数据(用于列引用求值)
     * @param columns 列定义(用于查找列)
     * @return 求值结果(Boolean/Number/String等)
     * @throws EvaluationException 求值失败
     */
    public Object eval(Expression expr, Row row, List<Column> columns) {
        if (expr == null) {
            throw new IllegalArgumentException("Expression cannot be null");
        }

        // 递归求值,根据表达式类型分发
        switch (expr.getType()) {
            case COLUMN:
                return evalColumn((ColumnExpression) expr, row, columns);

            case LITERAL:
                return evalLiteral((LiteralExpression) expr);

            case BINARY:
                return evalBinary((BinaryExpression) expr, row, columns);

            case NOT:
                return evalNot((NotExpression) expr, row, columns);

            default:
                throw new EvaluationException("Unknown expression type: " + expr.getType());
        }
    }

    /**
     * 求值表达式(便捷版本,自动从Table获取列定义)
     *
     * @param expr 表达式
     * @param row 行数据(如果为null,则只能求值字面量)
     * @param table 表对象(用于获取列定义)
     * @return 求值结果(Boolean/Number/String等)
     * @throws EvaluationException 求值失败
     */
    public Object evaluate(Expression expr, Row row, Table table) {
        if (expr == null) {
            throw new IllegalArgumentException("Expression cannot be null");
        }

        // 如果row为null,只能求值字面量
        if (row == null) {
            if (expr.getType() == Expression.ExpressionType.LITERAL) {
                return evalLiteral((LiteralExpression) expr);
            }
            throw new EvaluationException("Cannot evaluate expression without row context");
        }

        return eval(expr, row, table.getColumns());
    }

    /**
     * 求值表达式(简化版本,用于INSERT等场景)
     *
     * INSERT语句中的表达式通常是字面量,不需要Row上下文。
     *
     * @param expr 表达式
     * @param row 行数据(可以为null,仅用于字面量求值)
     * @return 求值结果
     * @throws EvaluationException 求值失败
     */
    public Object evaluate(Expression expr, Row row) {
        if (expr == null) {
            throw new IllegalArgumentException("Expression cannot be null");
        }

        // 如果row为null,只能求值字面量
        if (row == null) {
            if (expr.getType() == Expression.ExpressionType.LITERAL) {
                return evalLiteral((LiteralExpression) expr);
            }
            throw new EvaluationException("Cannot evaluate expression without row and table context");
        }

        // 需要Table上下文才能求值列引用
        throw new EvaluationException("Table context required for expression evaluation");
    }

    /**
     * 求值列引用表达式
     *
     * 从Row中获取列值,支持简单列名和带表名前缀的列名。
     *
     * @param expr 列引用表达式
     * @param row 行数据
     * @param columns 列定义
     * @return 列值
     */
    private Object evalColumn(ColumnExpression expr, Row row, List<Column> columns) {
        String columnName = expr.getColumnName();
        int columnIndex = Column.findIndex(columns, columnName);

        if (columnIndex < 0) {
            throw new EvaluationException("Column not found: " + columnName);
        }

        return row.getValue(columnIndex);
    }

    /**
     * 求值字面量表达式
     *
     * 直接返回字面量值。
     *
     * @param expr 字面量表达式
     * @return 字面量值
     */
    private Object evalLiteral(LiteralExpression expr) {
        return expr.getValue();
    }

    /**
     * 求值二元运算表达式
     *
     * 支持比较运算(=, !=, >, <, >=, <=)、逻辑运算(AND, OR)、算术运算(+, -, *, /, %)。
     *
     * @param expr 二元运算表达式
     * @param row 行数据
     * @param columns 列定义
     * @return 运算结果
     */
    private Object evalBinary(BinaryExpression expr, Row row, List<Column> columns) {
        Object left = eval(expr.getLeft(), row, columns);
        Object right = eval(expr.getRight(), row, columns);
        OperatorEnum op = expr.getOperator();

        if (op == OperatorEnum.AND || op == OperatorEnum.OR) {
            return evalLogical(op, left, right);
        }

        switch (op) {
            case EQUAL: case NOT_EQUAL: case GREATER_THAN: case LESS_THAN:
            case GREATER_EQUAL: case LESS_EQUAL:
                return evalComparison(op, left, right);
            case ADD: case SUBTRACT: case MULTIPLY: case DIVIDE: case MODULO:
                return evalArithmetic(op, left, right);
            default:
                throw new EvaluationException("Unsupported operator: " + op);
        }
    }

    private Object evalLogical(OperatorEnum op, Object left, Object right) {
        if (op == OperatorEnum.AND) {
            return toBoolean(left) && toBoolean(right);
        }
        return toBoolean(left) || toBoolean(right);
    }

    private Object evalComparison(OperatorEnum op, Object left, Object right) {
        int cmp = compare(left, right);
        switch (op) {
            case EQUAL:        return cmp == 0;
            case NOT_EQUAL:    return cmp != 0;
            case GREATER_THAN: return cmp > 0;
            case LESS_THAN:    return cmp < 0;
            case GREATER_EQUAL:return cmp >= 0;
            case LESS_EQUAL:   return cmp <= 0;
            default: throw new EvaluationException("Not a comparison operator: " + op);
        }
    }

    private Object evalArithmetic(OperatorEnum op, Object left, Object right) {
        ArithmeticOp arithOp;
        switch (op) {
            case ADD:      arithOp = ArithmeticOp.ADD; break;
            case SUBTRACT: arithOp = ArithmeticOp.SUBTRACT; break;
            case MULTIPLY: arithOp = ArithmeticOp.MULTIPLY; break;
            case DIVIDE:   arithOp = ArithmeticOp.DIVIDE; break;
            case MODULO:   arithOp = ArithmeticOp.MODULO; break;
            default: throw new EvaluationException("Not an arithmetic operator: " + op);
        }
        return arithmetic(left, right, arithOp);
    }

    /**
     * 求值NOT运算表达式
     *
     * @param expr NOT运算表达式
     * @param row 行数据
     * @param columns 列定义
     * @return 布尔取反结果
     */
    private Object evalNot(NotExpression expr, Row row, List<Column> columns) {
        Object operand = eval(expr.getOperand(), row, columns);
        return !toBoolean(operand);
    }

    /**
     * 比较两个值
     *
     * 支持Comparable类型(Integer, Long, Double, String等)。
     * 如果任一值为null,返回-1(null视为最小值)。
     *
     * @param left 左值
     * @param right 右值
     * @return 比较结果(left > right → >0, left == right → 0, left < right → <0)
     */
    @SuppressWarnings("unchecked")
    private int compare(Object left, Object right) {
        if (left == null || right == null) {
            return -1;
        }

        // 数值类型统一用 Double 比较
        if (left instanceof Number && right instanceof Number) {
            return Double.compare(
                    ((Number) left).doubleValue(),
                    ((Number) right).doubleValue()
            );
        }

        // 相同类型直接比较
        if (left.getClass() == right.getClass() && left instanceof Comparable) {
            return ((Comparable<Object>) left).compareTo(right);
        }

        throw new EvaluationException(
                "Cannot compare: " + left.getClass().getSimpleName() +
                        " vs " + right.getClass().getSimpleName());
    }

    /**
     * 转换为布尔值
     *
     * MySQL语义:
     * - 数字: 0 → false, 非0 → true
     * - 字符串: "" → false, 非空 → true
     * - null → false
     * - Boolean → 直接返回
     *
     * @param value 值
     * @return 布尔值
     */
    private boolean toBoolean(Object value) {
        if (value == null) {
            return false;
        }
        if (value instanceof Boolean) {
            return (Boolean) value;
        }
        if (value instanceof Number) {
            return ((Number) value).doubleValue() != 0;
        }
        if (value instanceof String) {
            return !((String) value).isEmpty();
        }
        throw new EvaluationException("Cannot convert to boolean: " + value.getClass().getSimpleName());
    }

    /**
     * 算术运算
     *
     * @param left 左值
     * @param right 右值
     * @param op 运算类型
     * @return 运算结果
     */
    private Object arithmetic(Object left, Object right, ArithmeticOp op) {
        if (left == null || right == null) {
            throw new EvaluationException("Arithmetic operation on null value");
        }

        // 混合数值类型：统一提升为 Double
        if (left instanceof Double || right instanceof Double) {
            double l = ((Number) left).doubleValue();
            double r = ((Number) right).doubleValue();

            return switch (op) {
                case ADD -> l + r;
                case SUBTRACT -> l - r;
                case MULTIPLY -> l * r;
                case DIVIDE -> l / r;
                case MODULO -> l % r;
            };
        }

        // 支持Integer和Long运算(Double可选)
        if (left instanceof Integer) {
            int l = (Integer) left;
            int r = (Integer) right;

            switch (op) {
                case ADD:
                    return l + r;
                case SUBTRACT:
                    return l - r;
                case MULTIPLY:
                    return l * r;
                case DIVIDE:
                    return l / r;
                case MODULO:
                    return l % r;
            }
        }

        if (left instanceof Long) {
            long l = (Long) left;
            long r = (Long) right;

            switch (op) {
                case ADD:
                    return l + r;
                case SUBTRACT:
                    return l - r;
                case MULTIPLY:
                    return l * r;
                case DIVIDE:
                    return l / r;
                case MODULO:
                    return l % r;
            }
        }

        if (left instanceof Double) {
            double l = (Double) left;
            double r = (Double) right;

            switch (op) {
                case ADD:
                    return l + r;
                case SUBTRACT:
                    return l - r;
                case MULTIPLY:
                    return l * r;
                case DIVIDE:
                    return l / r;
                case MODULO:
                    return l % r;
            }
        }

        throw new EvaluationException(
                "Arithmetic operation not supported for type: " + left.getClass().getSimpleName());
    }

    /**
     * 算术运算类型(内部枚举)
     */
    private enum ArithmeticOp {
        ADD, SUBTRACT, MULTIPLY, DIVIDE, MODULO
    }

    /**
     * 表达式求值异常
     */
    public static class EvaluationException extends RuntimeException {
        public EvaluationException(String message) {
            super(message);
        }

        public EvaluationException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
