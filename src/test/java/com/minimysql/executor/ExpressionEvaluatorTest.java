package com.minimysql.executor;

import com.minimysql.parser.Expression;
import com.minimysql.parser.expressions.*;
import com.minimysql.storage.table.Column;
import com.minimysql.storage.table.DataType;
import com.minimysql.storage.table.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * ExpressionEvaluatorTest - 表达式求值器测试
 *
 * 测试表达式求值器的各种功能:
 * - 列引用求值
 * - 字面量求值
 * - 比较运算
 * - 逻辑运算
 * - 算术运算
 * - NOT运算
 * - 复杂嵌套表达式
 */
@DisplayName("表达式求值器测试")
class ExpressionEvaluatorTest {

    private ExpressionEvaluator evaluator;
    private List<Column> columns;
    private Row row;

    @BeforeEach
    void setUp() {
        evaluator = new ExpressionEvaluator();

        // 创建列定义: id(INT), name(VARCHAR), age(INT), salary(DOUBLE)
        columns = Arrays.asList(
                new Column("id", DataType.INT, false),
                new Column("name", DataType.VARCHAR, 100, true),
                new Column("age", DataType.INT, false),
                new Column("salary", DataType.DOUBLE, false)
        );

        // 创建测试行: {id=1, name="Alice", age=25, salary=5000.0}
        Object[] values = {1, "Alice", 25, 5000.0};
        row = new Row(columns, values);
    }

    @Test
    @DisplayName("测试列引用求值")
    void testEvalColumn() {
        Expression expr = new ColumnExpression("age");
        Object result = evaluator.eval(expr, row, columns);

        assertEquals(25, result);
    }

    @Test
    @DisplayName("测试字面量求值")
    void testEvalLiteral() {
        Expression expr = new LiteralExpression(42);
        Object result = evaluator.eval(expr, row, columns);

        assertEquals(42, result);
    }

    @Test
    @DisplayName("测试等于运算")
    void testEvalEqual() {
        Expression expr = new BinaryExpression(
                new ColumnExpression("age"),
                com.minimysql.parser.expressions.Operator.EQUAL,
                new LiteralExpression(25)
        );

        Object result = evaluator.eval(expr, row, columns);

        assertTrue(result instanceof Boolean);
        assertTrue((Boolean) result);
    }

    @Test
    @DisplayName("测试不等于运算")
    void testEvalNotEqual() {
        Expression expr = new BinaryExpression(
                new ColumnExpression("age"),
                com.minimysql.parser.expressions.Operator.NOT_EQUAL,
                new LiteralExpression(30)
        );

        Object result = evaluator.eval(expr, row, columns);

        assertTrue(result instanceof Boolean);
        assertTrue((Boolean) result);
    }

    @Test
    @DisplayName("测试大于运算")
    void testEvalGreaterThan() {
        Expression expr = new BinaryExpression(
                new ColumnExpression("age"),
                com.minimysql.parser.expressions.Operator.GREATER_THAN,
                new LiteralExpression(18)
        );

        Object result = evaluator.eval(expr, row, columns);

        assertTrue(result instanceof Boolean);
        assertTrue((Boolean) result);
    }

    @Test
    @DisplayName("测试小于运算")
    void testEvalLessThan() {
        Expression expr = new BinaryExpression(
                new ColumnExpression("age"),
                com.minimysql.parser.expressions.Operator.LESS_THAN,
                new LiteralExpression(30)
        );

        Object result = evaluator.eval(expr, row, columns);

        assertTrue(result instanceof Boolean);
        assertTrue((Boolean) result);
    }

    @Test
    @DisplayName("测试大于等于运算")
    void testEvalGreaterEqual() {
        Expression expr = new BinaryExpression(
                new ColumnExpression("age"),
                com.minimysql.parser.expressions.Operator.GREATER_EQUAL,
                new LiteralExpression(25)
        );

        Object result = evaluator.eval(expr, row, columns);

        assertTrue(result instanceof Boolean);
        assertTrue((Boolean) result);
    }

    @Test
    @DisplayName("测试小于等于运算")
    void testEvalLessEqual() {
        Expression expr = new BinaryExpression(
                new ColumnExpression("age"),
                com.minimysql.parser.expressions.Operator.LESS_EQUAL,
                new LiteralExpression(25)
        );

        Object result = evaluator.eval(expr, row, columns);

        assertTrue(result instanceof Boolean);
        assertTrue((Boolean) result);
    }

    @Test
    @DisplayName("测试AND运算")
    void testEvalAnd() {
        Expression expr = new BinaryExpression(
                new BinaryExpression(
                        new ColumnExpression("age"),
                        com.minimysql.parser.expressions.Operator.GREATER_THAN,
                        new LiteralExpression(18)
                ),
                com.minimysql.parser.expressions.Operator.AND,
                new BinaryExpression(
                        new ColumnExpression("age"),
                        com.minimysql.parser.expressions.Operator.LESS_THAN,
                        new LiteralExpression(30)
                )
        );

        Object result = evaluator.eval(expr, row, columns);

        assertTrue(result instanceof Boolean);
        assertTrue((Boolean) result); // 25 > 18 AND 25 < 30 → true
    }

    @Test
    @DisplayName("测试OR运算")
    void testEvalOr() {
        Expression expr = new BinaryExpression(
                new BinaryExpression(
                        new ColumnExpression("age"),
                        com.minimysql.parser.expressions.Operator.LESS_THAN,
                        new LiteralExpression(18)
                ),
                com.minimysql.parser.expressions.Operator.OR,
                new BinaryExpression(
                        new ColumnExpression("age"),
                        com.minimysql.parser.expressions.Operator.GREATER_THAN,
                        new LiteralExpression(30)
                )
        );

        Object result = evaluator.eval(expr, row, columns);

        assertTrue(result instanceof Boolean);
        assertFalse((Boolean) result); // 25 < 18 OR 25 > 30 → false
    }

    @Test
    @DisplayName("测试NOT运算")
    void testEvalNot() {
        Expression expr = new NotExpression(
                new BinaryExpression(
                        new ColumnExpression("age"),
                        com.minimysql.parser.expressions.Operator.GREATER_THAN,
                        new LiteralExpression(30)
                )
        );

        Object result = evaluator.eval(expr, row, columns);

        assertTrue(result instanceof Boolean);
        assertTrue((Boolean) result); // NOT (25 > 30) → true
    }

    @Test
    @DisplayName("测试加法运算")
    void testEvalAdd() {
        Expression expr = new BinaryExpression(
                new ColumnExpression("age"),
                com.minimysql.parser.expressions.Operator.ADD,
                new LiteralExpression(5)
        );

        Object result = evaluator.eval(expr, row, columns);

        assertEquals(30, result);
    }

    @Test
    @DisplayName("测试减法运算")
    void testEvalSubtract() {
        Expression expr = new BinaryExpression(
                new ColumnExpression("age"),
                com.minimysql.parser.expressions.Operator.SUBTRACT,
                new LiteralExpression(5)
        );

        Object result = evaluator.eval(expr, row, columns);

        assertEquals(20, result);
    }

    @Test
    @DisplayName("测试乘法运算")
    void testEvalMultiply() {
        Expression expr = new BinaryExpression(
                new ColumnExpression("age"),
                com.minimysql.parser.expressions.Operator.MULTIPLY,
                new LiteralExpression(2)
        );

        Object result = evaluator.eval(expr, row, columns);

        assertEquals(50, result);
    }

    @Test
    @DisplayName("测试除法运算")
    void testEvalDivide() {
        Expression expr = new BinaryExpression(
                new ColumnExpression("age"),
                com.minimysql.parser.expressions.Operator.DIVIDE,
                new LiteralExpression(5)
        );

        Object result = evaluator.eval(expr, row, columns);

        assertEquals(5, result);
    }

    @Test
    @DisplayName("测试取模运算")
    void testEvalModulo() {
        Expression expr = new BinaryExpression(
                new ColumnExpression("age"),
                com.minimysql.parser.expressions.Operator.MODULO,
                new LiteralExpression(7)
        );

        Object result = evaluator.eval(expr, row, columns);

        assertEquals(4, result); // 25 % 7 = 4
    }

    @Test
    @DisplayName("测试复杂嵌套表达式")
    void testEvalComplexExpression() {
        // (age > 18 AND age < 30) OR (name = 'Bob')
        Expression expr = new BinaryExpression(
                new BinaryExpression(
                        new BinaryExpression(
                                new ColumnExpression("age"),
                                com.minimysql.parser.expressions.Operator.GREATER_THAN,
                                new LiteralExpression(18)
                        ),
                        com.minimysql.parser.expressions.Operator.AND,
                        new BinaryExpression(
                                new ColumnExpression("age"),
                                com.minimysql.parser.expressions.Operator.LESS_THAN,
                                new LiteralExpression(30)
                        )
                ),
                com.minimysql.parser.expressions.Operator.OR,
                new BinaryExpression(
                        new ColumnExpression("name"),
                        com.minimysql.parser.expressions.Operator.EQUAL,
                        new LiteralExpression("Bob")
                )
        );

        Object result = evaluator.eval(expr, row, columns);

        assertTrue(result instanceof Boolean);
        assertTrue((Boolean) result); // (25 > 18 AND 25 < 30) OR (Alice = Bob) → true OR false → true
    }

    @Test
    @DisplayName("测试列不存在抛异常")
    void testEvalColumnNotFound() {
        Expression expr = new ColumnExpression("unknown_column");

        assertThrows(ExpressionEvaluator.EvaluationException.class, () -> {
            evaluator.eval(expr, row, columns);
        });
    }

    @Test
    @DisplayName("测试类型不匹配抛异常")
    void testEvalTypeMismatch() {
        // 比较Integer和String
        Expression expr = new BinaryExpression(
                new ColumnExpression("age"),
                com.minimysql.parser.expressions.Operator.EQUAL,
                new LiteralExpression("twenty-five")
        );

        assertThrows(ExpressionEvaluator.EvaluationException.class, () -> {
            evaluator.eval(expr, row, columns);
        });
    }

    @Test
    @DisplayName("测试null值比较")
    void testEvalNullComparison() {
        // 创建包含null的行
        Object[] valuesWithNull = {1, null, 25, 5000.0};
        Row rowWithNull = new Row(columns, valuesWithNull);

        Expression expr = new BinaryExpression(
                new ColumnExpression("name"),
                com.minimysql.parser.expressions.Operator.EQUAL,
                new LiteralExpression("Alice")
        );

        Object result = evaluator.eval(expr, rowWithNull, columns);

        // null与任何值比较返回false
        assertTrue(result instanceof Boolean);
        assertFalse((Boolean) result);
    }
}
