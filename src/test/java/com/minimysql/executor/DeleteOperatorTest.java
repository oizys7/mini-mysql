package com.minimysql.executor;

import com.minimysql.executor.operator.DeleteOperator;
import com.minimysql.parser.Expression;
import com.minimysql.parser.expressions.BinaryExpression;
import com.minimysql.parser.expressions.ColumnExpression;
import com.minimysql.parser.expressions.LiteralExpression;
import com.minimysql.parser.expressions.OperatorEnum;
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
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * DeleteOperatorTest - DELETE删除算子测试
 *
 * 测试DELETE算子的功能:
 * - 删除单行
 * - 删除多行
 * - WHERE条件过滤
 * - 删除所有行
 * - 不匹配任何行的情况
 */
@DisplayName("DELETE删除算子测试")
class DeleteOperatorTest {

    private static final String TEST_DATA_DIR = "test_data_delete_operator";

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
    @DisplayName("测试删除单行(带WHERE条件)")
    void testDeleteSingleRow() {
        // DELETE FROM users WHERE id = 1
        Expression whereClause = new BinaryExpression(
                new ColumnExpression("id"),
                OperatorEnum.EQUAL,
                new LiteralExpression(1)
        );

        DeleteOperator deleteOp = new DeleteOperator(table, whereClause, evaluator);

        int affectedRows = deleteOp.execute();
        assertEquals(1, affectedRows);

        // 验证删除成功
        Row row = table.selectByPrimaryKey(1);
        assertNull(row);

        // 其他行仍然存在
        Row row2 = table.selectByPrimaryKey(2);
        assertNotNull(row2);
        assertEquals("Bob", row2.getValue(1));

        Row row3 = table.selectByPrimaryKey(3);
        assertNotNull(row3);
    }

    @Test
    @DisplayName("测试删除多行")
    void testDeleteMultipleRows() {
        // DELETE FROM users WHERE age > 25
        Expression whereClause = new BinaryExpression(
                new ColumnExpression("age"),
                OperatorEnum.GREATER_THAN,
                new LiteralExpression(25)
        );

        DeleteOperator deleteOp = new DeleteOperator(table, whereClause, evaluator);

        int affectedRows = deleteOp.execute();
        assertEquals(2, affectedRows); // id=2和id=3的行

        // 验证删除
        Row row1 = table.selectByPrimaryKey(1);
        Row row2 = table.selectByPrimaryKey(2);
        Row row3 = table.selectByPrimaryKey(3);

        assertNotNull(row1); // 未删除
        assertNull(row2);   // 已删除
        assertNull(row3);   // 已删除
    }

    @Test
    @DisplayName("测试删除所有行(无WHERE条件)")
    void testDeleteAllRows() {
        // DELETE FROM users
        DeleteOperator deleteOp = new DeleteOperator(table, null, evaluator);

        int affectedRows = deleteOp.execute();
        assertEquals(3, affectedRows);

        // 验证所有行都被删除
        Row row1 = table.selectByPrimaryKey(1);
        Row row2 = table.selectByPrimaryKey(2);
        Row row3 = table.selectByPrimaryKey(3);

        assertNull(row1);
        assertNull(row2);
        assertNull(row3);
    }

    @Test
    @DisplayName("测试删除不匹配任何行")
    void testDeleteNoRowsMatched() {
        // DELETE FROM users WHERE id = 999
        Expression whereClause = new BinaryExpression(
                new ColumnExpression("id"),
                OperatorEnum.EQUAL,
                new LiteralExpression(999)
        );

        DeleteOperator deleteOp = new DeleteOperator(table, whereClause, evaluator);

        int affectedRows = deleteOp.execute();
        assertEquals(0, affectedRows);

        // 验证没有行被删除
        Row row1 = table.selectByPrimaryKey(1);
        Row row2 = table.selectByPrimaryKey(2);
        Row row3 = table.selectByPrimaryKey(3);

        assertNotNull(row1);
        assertNotNull(row2);
        assertNotNull(row3);
    }

    @Test
    @DisplayName("测试获取删除的主键列表")
    void testGetDeletedPrimaryKeys() {
        // DELETE FROM users WHERE age > 25
        Expression whereClause = new BinaryExpression(
                new ColumnExpression("age"),
                OperatorEnum.GREATER_THAN,
                new LiteralExpression(25)
        );

        DeleteOperator deleteOp = new DeleteOperator(table, whereClause, evaluator);

        deleteOp.execute();

        List<Object> deletedKeys = deleteOp.getDeletedPrimaryKeys();
        assertEquals(2, deletedKeys.size());
        assertTrue(deletedKeys.contains(2));
        assertTrue(deletedKeys.contains(3));
    }

    @Test
    @DisplayName("测试DELETE后全表扫描")
    void testDeleteThenFullTableScan() {
        // DELETE FROM users WHERE id = 2
        Expression whereClause = new BinaryExpression(
                new ColumnExpression("id"),
                OperatorEnum.EQUAL,
                new LiteralExpression(2)
        );

        DeleteOperator deleteOp = new DeleteOperator(table, whereClause, evaluator);
        deleteOp.execute();

        // 全表扫描应该只返回2行
        List<Row> allRows = table.fullTableScan();
        assertEquals(2, allRows.size());

        // 验证剩下的行是id=1和id=3
        assertTrue(allRows.stream().anyMatch(r -> r.getValue(0).equals(1)));
        assertTrue(allRows.stream().anyMatch(r -> r.getValue(0).equals(3)));
    }

    @Test
    @DisplayName("测试多次删除同一行")
    void testDeleteSameRowTwice() {
        // 第一次删除
        Expression whereClause = new BinaryExpression(
                new ColumnExpression("id"),
                OperatorEnum.EQUAL,
                new LiteralExpression(1)
        );

        DeleteOperator deleteOp1 = new DeleteOperator(table, whereClause, evaluator);
        int affectedRows1 = deleteOp1.execute();
        assertEquals(1, affectedRows1);

        // 第二次删除同一行
        DeleteOperator deleteOp2 = new DeleteOperator(table, whereClause, evaluator);
        int affectedRows2 = deleteOp2.execute();
        assertEquals(0, affectedRows2); // 没有行被删除
    }

    @Test
    @DisplayName("测试复杂WHERE条件")
    void testDeleteWithComplexWhere() {
        // DELETE FROM users WHERE age > 25 AND age < 35
        Expression whereClause = new BinaryExpression(
                new BinaryExpression(
                        new ColumnExpression("age"),
                        OperatorEnum.GREATER_THAN,
                        new LiteralExpression(25)
                ),
                OperatorEnum.AND,
                new BinaryExpression(
                        new ColumnExpression("age"),
                        OperatorEnum.LESS_THAN,
                        new LiteralExpression(35)
                )
        );

        DeleteOperator deleteOp = new DeleteOperator(table, whereClause, evaluator);

        int affectedRows = deleteOp.execute();
        assertEquals(1, affectedRows); // 只有id=2的行(age=30)

        // 验证
        Row row1 = table.selectByPrimaryKey(1);
        Row row2 = table.selectByPrimaryKey(2);
        Row row3 = table.selectByPrimaryKey(3);

        assertNotNull(row1); // age=25,不满足age > 25
        assertNull(row2);   // age=30,满足条件
        assertNotNull(row3); // age=35,不满足age < 35
    }

    @Test
    @DisplayName("测试重复执行抛出异常")
    void testDeleteTwiceThrowsException() {
        Expression whereClause = new BinaryExpression(
                new ColumnExpression("id"),
                OperatorEnum.EQUAL,
                new LiteralExpression(1)
        );

        DeleteOperator deleteOp = new DeleteOperator(table, whereClause, evaluator);

        deleteOp.execute();

        // 第二次执行应该抛出异常
        assertThrows(IllegalStateException.class, deleteOp::execute);
    }

    @Test
    @DisplayName("测试构造函数空指针检查")
    void testConstructorNullChecks() {
        // table为null
        assertThrows(IllegalArgumentException.class, () -> {
            new DeleteOperator(null, null, evaluator);
        });

        // evaluator为null
        assertThrows(IllegalArgumentException.class, () -> {
            new DeleteOperator(table, null, null);
        });
    }

    @Test
    @DisplayName("测试不支持迭代操作")
    void testNotSupportIteration() {
        DeleteOperator deleteOp = new DeleteOperator(table, null, evaluator);

        assertFalse(deleteOp.hasNext());
        assertThrows(UnsupportedOperationException.class, deleteOp::next);
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
