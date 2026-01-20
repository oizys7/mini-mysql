package com.minimysql.executor;

import com.minimysql.executor.ExpressionEvaluator;
import com.minimysql.executor.operator.UpdateOperator;
import com.minimysql.parser.expressions.BinaryExpression;
import com.minimysql.parser.expressions.ColumnExpression;
import com.minimysql.parser.expressions.LiteralExpression;
import com.minimysql.parser.expressions.Operator;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * UpdateOperatorTest - UPDATE更新算子测试
 *
 * 测试UPDATE算子的功能:
 * - 更新单行
 * - 更新多行
 * - WHERE条件过滤
 * - 多列更新
 * - 类型检查和转换
 */
@DisplayName("UPDATE更新算子测试")
class UpdateOperatorTest {

    private static final String TEST_DATA_DIR = "test_data_update_operator";

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

        // 插入测试数据
        table.insertRow(new Row(table.getColumns(), new Object[]{1, "Alice", 25}));
        table.insertRow(new Row(table.getColumns(), new Object[]{2, "Bob", 30}));
        table.insertRow(new Row(table.getColumns(), new Object[]{3, "Charlie", 35}));
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
    @DisplayName("测试更新单行(带WHERE条件)")
    void testUpdateSingleRow() {
        // UPDATE users SET age = 26 WHERE id = 1
        Map<String, com.minimysql.parser.Expression> assignments = new HashMap<>();
        assignments.put("age", new LiteralExpression(26));

        com.minimysql.parser.Expression whereClause = new BinaryExpression(
                new ColumnExpression("id"),
                Operator.EQUAL,
                new LiteralExpression(1)
        );

        UpdateOperator updateOp = new UpdateOperator(table, assignments, whereClause, evaluator);

        int affectedRows = updateOp.execute();
        assertEquals(1, affectedRows);

        // 验证更新成功
        Row row = table.selectByPrimaryKey(1);
        assertEquals(26, row.getValue(2));
        assertEquals("Alice", row.getValue(1)); // name列未改变

        // 其他行未改变
        Row row2 = table.selectByPrimaryKey(2);
        assertEquals(30, row2.getValue(2));
    }

    @Test
    @DisplayName("测试更新多行")
    void testUpdateMultipleRows() {
        // UPDATE users SET age = 100 WHERE age > 25
        Map<String, com.minimysql.parser.Expression> assignments = new HashMap<>();
        assignments.put("age", new LiteralExpression(100));

        com.minimysql.parser.Expression whereClause = new BinaryExpression(
                new ColumnExpression("age"),
                Operator.GREATER_THAN,
                new LiteralExpression(25)
        );

        UpdateOperator updateOp = new UpdateOperator(table, assignments, whereClause, evaluator);

        int affectedRows = updateOp.execute();
        assertEquals(2, affectedRows); // id=2和id=3的行

        // 验证更新
        Row row1 = table.selectByPrimaryKey(1);
        Row row2 = table.selectByPrimaryKey(2);
        Row row3 = table.selectByPrimaryKey(3);

        assertEquals(25, row1.getValue(2)); // 未更新
        assertEquals(100, row2.getValue(2)); // 已更新
        assertEquals(100, row3.getValue(2)); // 已更新
    }

    @Test
    @DisplayName("测试更新多列")
    void testUpdateMultipleColumns() {
        // UPDATE users SET name = 'Alice Updated', age = 26 WHERE id = 1
        Map<String, com.minimysql.parser.Expression> assignments = new HashMap<>();
        assignments.put("name", new LiteralExpression("Alice Updated"));
        assignments.put("age", new LiteralExpression(26));

        com.minimysql.parser.Expression whereClause = new BinaryExpression(
                new ColumnExpression("id"),
                Operator.EQUAL,
                new LiteralExpression(1)
        );

        UpdateOperator updateOp = new UpdateOperator(table, assignments, whereClause, evaluator);

        int affectedRows = updateOp.execute();
        assertEquals(1, affectedRows);

        // 验证更新
        Row row = table.selectByPrimaryKey(1);
        assertEquals("Alice Updated", row.getValue(1));
        assertEquals(26, row.getValue(2));
    }

    @Test
    @DisplayName("测试更新所有行(无WHERE条件)")
    void testUpdateAllRows() {
        // UPDATE users SET age = 50
        Map<String, com.minimysql.parser.Expression> assignments = new HashMap<>();
        assignments.put("age", new LiteralExpression(50));

        UpdateOperator updateOp = new UpdateOperator(table, assignments, null, evaluator);

        int affectedRows = updateOp.execute();
        assertEquals(3, affectedRows);

        // 验证所有行都更新了
        Row row1 = table.selectByPrimaryKey(1);
        Row row2 = table.selectByPrimaryKey(2);
        Row row3 = table.selectByPrimaryKey(3);

        assertEquals(50, row1.getValue(2));
        assertEquals(50, row2.getValue(2));
        assertEquals(50, row3.getValue(2));
    }

    @Test
    @DisplayName("测试更新不匹配任何行")
    void testUpdateNoRowsMatched() {
        // UPDATE users SET age = 100 WHERE id = 999
        Map<String, com.minimysql.parser.Expression> assignments = new HashMap<>();
        assignments.put("age", new LiteralExpression(100));

        com.minimysql.parser.Expression whereClause = new BinaryExpression(
                new ColumnExpression("id"),
                Operator.EQUAL,
                new LiteralExpression(999)
        );

        UpdateOperator updateOp = new UpdateOperator(table, assignments, whereClause, evaluator);

        int affectedRows = updateOp.execute();
        assertEquals(0, affectedRows);
    }

    @Test
    @DisplayName("测试更新失败:尝试更新主键列")
    void testUpdatePrimaryKeyColumn() {
        // 尝试更新id列(主键)
        Map<String, com.minimysql.parser.Expression> assignments = new HashMap<>();
        assignments.put("id", new LiteralExpression(999));

        com.minimysql.parser.Expression whereClause = new BinaryExpression(
                new ColumnExpression("id"),
                Operator.EQUAL,
                new LiteralExpression(1)
        );

        UpdateOperator updateOp = new UpdateOperator(table, assignments, whereClause, evaluator);

        assertThrows(IllegalArgumentException.class, updateOp::execute);
    }

    @Test
    @DisplayName("测试更新失败:列名不存在")
    void testUpdateColumnNotFound() {
        // 指定一个不存在的列
        Map<String, com.minimysql.parser.Expression> assignments = new HashMap<>();
        assignments.put("invalid_column", new LiteralExpression(100));

        com.minimysql.parser.Expression whereClause = new BinaryExpression(
                new ColumnExpression("id"),
                Operator.EQUAL,
                new LiteralExpression(1)
        );

        UpdateOperator updateOp = new UpdateOperator(table, assignments, whereClause, evaluator);

        assertThrows(IllegalArgumentException.class, updateOp::execute);
    }

    @Test
    @DisplayName("测试更新包含NULL值")
    void testUpdateWithNull() {
        // UPDATE users SET name = NULL WHERE id = 1
        Map<String, com.minimysql.parser.Expression> assignments = new HashMap<>();
        assignments.put("name", new LiteralExpression(null));

        com.minimysql.parser.Expression whereClause = new BinaryExpression(
                new ColumnExpression("id"),
                Operator.EQUAL,
                new LiteralExpression(1)
        );

        UpdateOperator updateOp = new UpdateOperator(table, assignments, whereClause, evaluator);

        int affectedRows = updateOp.execute();
        assertEquals(1, affectedRows);

        // 验证name列变为NULL
        Row row = table.selectByPrimaryKey(1);
        assertNull(row.getValue(1));
    }

    @Test
    @DisplayName("测试更新失败:向NOT NULL列设置NULL")
    void testUpdateNullToNotNullColumn() {
        // 尝试将age列(NOT NULL)设置为NULL
        Map<String, com.minimysql.parser.Expression> assignments = new HashMap<>();
        assignments.put("age", new LiteralExpression(null));

        com.minimysql.parser.Expression whereClause = new BinaryExpression(
                new ColumnExpression("id"),
                Operator.EQUAL,
                new LiteralExpression(1)
        );

        UpdateOperator updateOp = new UpdateOperator(table, assignments, whereClause, evaluator);

        assertThrows(IllegalArgumentException.class, updateOp::execute);
    }

    @Test
    @DisplayName("测试类型自动转换")
    void testUpdateTypeConversion() {
        // age列是INT,传入String类型的数字
        Map<String, com.minimysql.parser.Expression> assignments = new HashMap<>();
        assignments.put("age", new LiteralExpression("26")); // String类型,应该自动转换为INT

        com.minimysql.parser.Expression whereClause = new BinaryExpression(
                new ColumnExpression("id"),
                Operator.EQUAL,
                new LiteralExpression(1)
        );

        UpdateOperator updateOp = new UpdateOperator(table, assignments, whereClause, evaluator);

        int affectedRows = updateOp.execute();
        assertEquals(1, affectedRows);

        // 验证age列是整数类型
        Row row = table.selectByPrimaryKey(1);
        assertEquals(26, row.getValue(2));
        assertEquals(Integer.class, row.getValue(2).getClass());
    }

    @Test
    @DisplayName("测试重复执行抛出异常")
    void testUpdateTwiceThrowsException() {
        Map<String, com.minimysql.parser.Expression> assignments = new HashMap<>();
        assignments.put("age", new LiteralExpression(26));

        com.minimysql.parser.Expression whereClause = new BinaryExpression(
                new ColumnExpression("id"),
                Operator.EQUAL,
                new LiteralExpression(1)
        );

        UpdateOperator updateOp = new UpdateOperator(table, assignments, whereClause, evaluator);

        updateOp.execute();

        // 第二次执行应该抛出异常
        assertThrows(IllegalStateException.class, updateOp::execute);
    }

    @Test
    @DisplayName("测试构造函数空指针检查")
    void testConstructorNullChecks() {
        Map<String, com.minimysql.parser.Expression> assignments = new HashMap<>();
        assignments.put("age", new LiteralExpression(26));

        // table为null
        assertThrows(IllegalArgumentException.class, () -> {
            new UpdateOperator(null, assignments, null, evaluator);
        });

        // assignments为null
        assertThrows(IllegalArgumentException.class, () -> {
            new UpdateOperator(table, null, null, evaluator);
        });

        // assignments为空
        assertThrows(IllegalArgumentException.class, () -> {
            new UpdateOperator(table, new HashMap<>(), null, evaluator);
        });

        // evaluator为null
        assertThrows(IllegalArgumentException.class, () -> {
            new UpdateOperator(table, assignments, null, null);
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
