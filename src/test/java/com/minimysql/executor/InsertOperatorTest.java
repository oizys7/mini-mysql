package com.minimysql.executor;

import com.minimysql.executor.ExpressionEvaluator;
import com.minimysql.executor.operator.InsertOperator;
import com.minimysql.parser.Expression;
import com.minimysql.parser.expressions.LiteralExpression;
import com.minimysql.storage.buffer.BufferPool;
import com.minimysql.storage.impl.InnoDBStorageEngine;
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
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * InsertOperatorTest - INSERT插入算子测试
 *
 * 测试INSERT算子的功能:
 * - 插入单行数据
 * - 插入多行数据
 * - 指定列名插入
 * - 类型检查和转换
 * - 空值处理
 */
@DisplayName("INSERT插入算子测试")
class InsertOperatorTest {

    private static final String TEST_DATA_DIR = "test_data_insert_operator";

    private BufferPool bufferPool;
    private InnoDBStorageEngine storageEngine;
    private Table table;
    private ExpressionEvaluator evaluator;

    @BeforeEach
    void setUp() {
        // 清理测试数据
        cleanupTestData();

        // 创建BufferPool
        bufferPool = new BufferPool(10);

        // 创建StorageEngine (不使用元数据持久化)
        storageEngine = new InnoDBStorageEngine(10, false);

        // 创建表: users(id INT, name VARCHAR(100), age INT)
        List<Column> columns = Arrays.asList(
                new Column("id", DataType.INT, false),
                new Column("name", DataType.VARCHAR, 100, true),
                new Column("age", DataType.INT, false)
        );

        storageEngine.createTable("users", columns);

        // 获取表对象
        table = storageEngine.getTable("users");

        // 创建表达式求值器
        evaluator = new ExpressionEvaluator();
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
    @DisplayName("测试插入单行数据(按表定义顺序)")
    void testInsertSingleRow() {
        // INSERT INTO users VALUES (1, 'Alice', 25)
        List<List<Expression>> values = List.of(
                List.of(
                        new LiteralExpression(1),
                        new LiteralExpression("Alice"),
                        new LiteralExpression(25)
                )
        );

        InsertOperator insertOp = new InsertOperator(
                table,
                Collections.emptyList(),
                values,
                evaluator
        );

        int affectedRows = insertOp.execute();
        assertEquals(1, affectedRows);

        // 验证插入成功
        Row row = table.selectByPrimaryKey(1);
        assertNotNull(row);
        assertEquals(1, row.getValue(0));
        assertEquals("Alice", row.getValue(1));
        assertEquals(25, row.getValue(2));
    }

    @Test
    @DisplayName("测试插入多行数据")
    void testInsertMultipleRows() {
        // INSERT INTO users VALUES (1, 'Alice', 25), (2, 'Bob', 30), (3, 'Charlie', 35)
        List<List<Expression>> values = List.of(
                List.of(new LiteralExpression(1), new LiteralExpression("Alice"), new LiteralExpression(25)),
                List.of(new LiteralExpression(2), new LiteralExpression("Bob"), new LiteralExpression(30)),
                List.of(new LiteralExpression(3), new LiteralExpression("Charlie"), new LiteralExpression(35))
        );

        InsertOperator insertOp = new InsertOperator(
                table,
                Collections.emptyList(),
                values,
                evaluator
        );

        int affectedRows = insertOp.execute();
        assertEquals(3, affectedRows);

        // 验证所有行都插入成功
        Row row1 = table.selectByPrimaryKey(1);
        Row row2 = table.selectByPrimaryKey(2);
        Row row3 = table.selectByPrimaryKey(3);

        assertNotNull(row1);
        assertNotNull(row2);
        assertNotNull(row3);

        assertEquals("Alice", row1.getValue(1));
        assertEquals("Bob", row2.getValue(1));
        assertEquals("Charlie", row3.getValue(1));
    }

    @Test
    @DisplayName("测试指定列名插入")
    void testInsertWithColumnNames() {
        // INSERT INTO users (id, age) VALUES (1, 25)
        List<String> columnNames = Arrays.asList("id", "age");
        List<List<Expression>> values = List.of(
                List.of(
                        new LiteralExpression(1),
                        new LiteralExpression(25)
                )
        );

        InsertOperator insertOp = new InsertOperator(
                table,
                columnNames,
                values,
                evaluator
        );

        int affectedRows = insertOp.execute();
        assertEquals(1, affectedRows);

        // 验证插入成功(name列应该是null)
        Row row = table.selectByPrimaryKey(1);
        assertNotNull(row);
        assertEquals(1, row.getValue(0));
        assertNull(row.getValue(1)); // name列
        assertEquals(25, row.getValue(2));
    }

    @Test
    @DisplayName("测试插入包含NULL值")
    void testInsertWithNull() {
        // INSERT INTO users VALUES (1, NULL, 25)
        List<List<Expression>> values = List.of(
                List.of(
                        new LiteralExpression(1),
                        new LiteralExpression(null),
                        new LiteralExpression(25)
                )
        );

        InsertOperator insertOp = new InsertOperator(
                table,
                Collections.emptyList(),
                values,
                evaluator
        );

        int affectedRows = insertOp.execute();
        assertEquals(1, affectedRows);

        // 验证name列为NULL
        Row row = table.selectByPrimaryKey(1);
        assertNotNull(row);
        assertNull(row.getValue(1));
    }

    @Test
    @DisplayName("测试插入失败:列数不匹配")
    void testInsertColumnCountMismatch() {
        // 提供的值太少
        List<List<Expression>> values = List.of(
                List.of(
                        new LiteralExpression(1),
                        new LiteralExpression("Alice")
                )
        );

        InsertOperator insertOp = new InsertOperator(
                table,
                Collections.emptyList(),
                values,
                evaluator
        );

        assertThrows(IllegalArgumentException.class, insertOp::execute);
    }

    @Test
    @DisplayName("测试插入失败:向NOT NULL列插入NULL")
    void testInsertNullToNotNullColumn() {
        // id列是NOT NULL,尝试插入NULL
        List<List<Expression>> values = List.of(
                List.of(
                        new LiteralExpression(null),
                        new LiteralExpression("Alice"),
                        new LiteralExpression(25)
                )
        );

        InsertOperator insertOp = new InsertOperator(
                table,
                Collections.emptyList(),
                values,
                evaluator
        );

        assertThrows(IllegalArgumentException.class, insertOp::execute);
    }

    @Test
    @DisplayName("测试插入失败:列名不存在")
    void testInsertColumnNotFound() {
        // 指定一个不存在的列名
        List<String> columnNames = Arrays.asList("id", "invalid_column");
        List<List<Expression>> values = List.of(
                List.of(
                        new LiteralExpression(1),
                        new LiteralExpression(25)
                )
        );

        InsertOperator insertOp = new InsertOperator(
                table,
                columnNames,
                values,
                evaluator
        );

        assertThrows(IllegalArgumentException.class, insertOp::execute);
    }

    @Test
    @DisplayName("测试类型自动转换")
    void testInsertTypeConversion() {
        // age列是INT,传入String类型的数字
        List<List<Expression>> values = List.of(
                List.of(
                        new LiteralExpression(1),
                        new LiteralExpression("Alice"),
                        new LiteralExpression("25") // String类型,应该自动转换为INT
                )
        );

        InsertOperator insertOp = new InsertOperator(
                table,
                Collections.emptyList(),
                values,
                evaluator
        );

        int affectedRows = insertOp.execute();
        assertEquals(1, affectedRows);

        // 验证age列是整数类型
        Row row = table.selectByPrimaryKey(1);
        assertNotNull(row);
        assertEquals(25, row.getValue(2));
        assertEquals(Integer.class, row.getValue(2).getClass());
    }

    @Test
    @DisplayName("测试重复执行抛出异常")
    void testInsertTwiceThrowsException() {
        List<List<Expression>> values = List.of(
                List.of(new LiteralExpression(1), new LiteralExpression("Alice"), new LiteralExpression(25))
        );

        InsertOperator insertOp = new InsertOperator(
                table,
                Collections.emptyList(),
                values,
                evaluator
        );

        insertOp.execute();

        // 第二次执行应该抛出异常
        assertThrows(IllegalStateException.class, insertOp::execute);
    }

    @Test
    @DisplayName("测试构造函数空指针检查")
    void testConstructorNullChecks() {
        List<List<Expression>> values = List.of(
                List.of(new LiteralExpression(1), new LiteralExpression("Alice"), new LiteralExpression(25))
        );

        // table为null
        assertThrows(IllegalArgumentException.class, () -> {
            new InsertOperator(null, Collections.emptyList(), values, evaluator);
        });

        // values为null
        assertThrows(IllegalArgumentException.class, () -> {
            new InsertOperator(table, Collections.emptyList(), null, evaluator);
        });

        // values为空
        assertThrows(IllegalArgumentException.class, () -> {
            new InsertOperator(table, Collections.emptyList(), Collections.emptyList(), evaluator);
        });

        // evaluator为null
        assertThrows(IllegalArgumentException.class, () -> {
            new InsertOperator(table, Collections.emptyList(), values, null);
        });
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
