package com.minimysql.executor;

import com.minimysql.executor.operator.FilterOperator;
import com.minimysql.executor.operator.ScanOperator;
import com.minimysql.parser.Expression;
import com.minimysql.parser.expressions.BinaryExpression;
import com.minimysql.parser.expressions.ColumnExpression;
import com.minimysql.parser.expressions.LiteralExpression;
import com.minimysql.parser.expressions.OperatorEnum;
import com.minimysql.storage.StorageEngine;
import com.minimysql.storage.StorageEngineFactory;
import com.minimysql.storage.table.Column;
import com.minimysql.storage.table.DataType;
import com.minimysql.storage.table.Row;
import com.minimysql.storage.table.Table;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * FilterOperatorTest - WHERE条件过滤算子测试
 *
 * 测试过滤算子的功能:
 * - 简单条件过滤(age > 18)
 * - 复杂条件过滤(age > 18 AND age < 30)
 * - 无符合条件的行
 * - 所有行都符合条件
 * - hasNext()和next()的正确性
 */
@DisplayName("WHERE条件过滤算子测试")
class FilterOperatorTest {

    private static final String TEST_DATA_DIR = "test_data_filter_operator";

    private StorageEngine storageEngine;
    private Table table;
    private ScanOperator scan;

    @BeforeEach
    void setUp() {
        // 清理测试数据
        cleanupTestData();

        // 创建BufferPool

        // 创建StorageEngine
        storageEngine = StorageEngineFactory.createEngine(
                StorageEngineFactory.EngineType.INNODB,
                10,
                false,
                TEST_DATA_DIR
        );

        // 创建表: users(id INT, name VARCHAR(100), age INT)
        List<Column> columns = Arrays.asList(
                new Column("id", DataType.INT, false),
                new Column("name", DataType.VARCHAR, 100, true),
                new Column("age", DataType.INT, false)
        );

        storageEngine.createTable("users", columns);

        // 获取表对象
        table = storageEngine.getTable("users");

        // 插入测试数据
        table.insertRow(new Row(new Object[]{1, "Alice", 25}));
        table.insertRow(new Row(new Object[]{2, "Bob", 17}));
        table.insertRow(new Row(new Object[]{3, "Charlie", 30}));
        table.insertRow(new Row(new Object[]{4, "David", 15}));
        table.insertRow(new Row(new Object[]{5, "Eve", 35}));

        // 创建ScanOperator
        scan = new ScanOperator(table);
    }

    @AfterEach
    void tearDown() {
        // 关闭StorageEngine
        if (storageEngine != null) {
            storageEngine.close();
        }

        // 清理测试数据
        cleanupTestData();
    }

    @Test
    @DisplayName("测试简单条件过滤 - age > 18")
    void testSimpleFilter() {
        // WHERE age > 18
        Expression whereCondition = new BinaryExpression(
                new ColumnExpression("age"),
                OperatorEnum.GREATER_THAN,
                new LiteralExpression(18)
        );

        FilterOperator filter = new FilterOperator(scan, whereCondition, table.getColumns());

        // 应该返回3行: Alice(25), Charlie(30), Eve(35)
        assertTrue(filter.hasNext());
        Row row1 = filter.next();
        assertEquals(25, row1.getValue(0));

        assertTrue(filter.hasNext());
        Row row2 = filter.next();
        assertEquals(30, row2.getValue(0));

        assertTrue(filter.hasNext());
        Row row3 = filter.next();
        assertEquals(35, row3.getValue(0));

        assertFalse(filter.hasNext());
    }

    @Test
    @DisplayName("测试复杂条件过滤 - age > 18 AND age < 30")
    void testComplexFilter() {
        // WHERE age > 18 AND age < 30
        Expression whereCondition = new BinaryExpression(
                new BinaryExpression(
                        new ColumnExpression("age"),
                        OperatorEnum.GREATER_THAN,
                        new LiteralExpression(18)
                ),
                OperatorEnum.AND,
                new BinaryExpression(
                        new ColumnExpression("age"),
                        OperatorEnum.LESS_THAN,
                        new LiteralExpression(30)
                )
        );

        FilterOperator filter = new FilterOperator(scan, whereCondition, table.getColumns());

        // 应该只返回1行: Alice(25)
        assertTrue(filter.hasNext());
        Row row = filter.next();
        assertEquals(25, row.getValue(0));
        assertEquals("Alice", row.getValue("name"));

        assertFalse(filter.hasNext());
    }

    @Test
    @DisplayName("测试无符合条件的行")
    void testNoMatchingRows() {
        // WHERE age > 100
        Expression whereCondition = new BinaryExpression(
                new ColumnExpression("age"),
                OperatorEnum.GREATER_THAN,
                new LiteralExpression(100)
        );

        FilterOperator filter = new FilterOperator(scan, whereCondition, table.getColumns());

        assertFalse(filter.hasNext());
    }

    @Test
    @DisplayName("测试所有行都符合条件")
    void testAllRowsMatch() {
        // WHERE age > 10
        Expression whereCondition = new BinaryExpression(
                new ColumnExpression("age"),
                OperatorEnum.GREATER_THAN,
                new LiteralExpression(10)
        );

        FilterOperator filter = new FilterOperator(scan, whereCondition, table.getColumns());

        // 应该返回所有5行
        int count = 0;
        while (filter.hasNext()) {
            filter.next();
            count++;
        }

        assertEquals(5, count);
    }

    @Test
    @DisplayName("测试等于条件")
    void testEqualsCondition() {
        // WHERE age = 25
        Expression whereCondition = new BinaryExpression(
                new ColumnExpression("age"),
                OperatorEnum.EQUAL,
                new LiteralExpression(25)
        );

        FilterOperator filter = new FilterOperator(scan, whereCondition, table.getColumns());

        assertTrue(filter.hasNext());
        Row row = filter.next();
        assertEquals(25, row.getValue(0));
        assertEquals("Alice", row.getValue("name"));

        assertFalse(filter.hasNext());
    }

    @Test
    @DisplayName("测试OR条件")
    void testOrCondition() {
        // WHERE age < 18 OR age > 30
        Expression whereCondition = new BinaryExpression(
                new BinaryExpression(
                        new ColumnExpression("age"),
                        OperatorEnum.LESS_THAN,
                        new LiteralExpression(18)
                ),
                OperatorEnum.OR,
                new BinaryExpression(
                        new ColumnExpression("age"),
                        OperatorEnum.GREATER_THAN,
                        new LiteralExpression(30)
                )
        );

        FilterOperator filter = new FilterOperator(scan, whereCondition, table.getColumns());

        // 应该返回3行: Bob(17), David(15), Eve(35)
        int count = 0;
        while (filter.hasNext()) {
            Row row = filter.next();
            count++;
            assertTrue((Integer) row.getValue(0) < 18 || (Integer) row.getValue(0) > 30);
        }

        assertEquals(3, count);
    }

    @Test
    @DisplayName("测试next()在没有更多行时抛异常")
    void testNextThrowsExceptionWhenNoMoreRows() {
        // WHERE age > 100 (无符合条件的行)
        Expression whereCondition = new BinaryExpression(
                new ColumnExpression("age"),
                OperatorEnum.GREATER_THAN,
                new LiteralExpression(100)
        );

        FilterOperator filter = new FilterOperator(scan, whereCondition, table.getColumns());

        assertFalse(filter.hasNext());
        assertThrows(java.util.NoSuchElementException.class, () -> {
            filter.next();
        });
    }

    @Test
    @DisplayName("测试FilterOperator构造函数空指针检查")
    void testConstructorNullChecks() {
        Expression whereCondition = new BinaryExpression(
                new ColumnExpression("age"),
                OperatorEnum.GREATER_THAN,
                new LiteralExpression(18)
        );

        // child为null
        assertThrows(IllegalArgumentException.class, () -> {
            new FilterOperator(null, whereCondition, table.getColumns());
        });

        // whereCondition为null
        assertThrows(IllegalArgumentException.class, () -> {
            new FilterOperator(scan, null, table.getColumns());
        });

        // columns为null
        assertThrows(IllegalArgumentException.class, () -> {
            new FilterOperator(scan, whereCondition, null);
        });
    }

    @Test
    @DisplayName("测试getChild()和getWhereCondition()")
    void testGetters() {
        Expression whereCondition = new BinaryExpression(
                new ColumnExpression("age"),
                OperatorEnum.GREATER_THAN,
                new LiteralExpression(18)
        );

        FilterOperator filter = new FilterOperator(scan, whereCondition, table.getColumns());

        assertEquals(scan, filter.getChild());
        assertEquals(whereCondition, filter.getWhereCondition());
    }

    /**
     * 清理测试数据
     */
    private void cleanupTestData() {
        File dir = new File(TEST_DATA_DIR);
        if (dir.exists()) {
            File[] files = dir.listFiles();
            if (files != null) {
                for (File file : files) {
                    file.delete();
                }
            }
            dir.delete();
        }
    }
}
