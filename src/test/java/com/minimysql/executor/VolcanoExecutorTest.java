package com.minimysql.executor;

import com.minimysql.parser.SQLParser;
import com.minimysql.parser.Statement;
import com.minimysql.result.QueryResult;
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
 * VolcanoExecutorTest - 火山模型执行器测试
 *
 * 测试执行器的功能:
 * - 执行SELECT *查询
 * - 执行SELECT指定列查询
 * - 执行带WHERE条件的查询
 * - 执行复杂查询
 * - 错误处理(表不存在)
 */
@DisplayName("火山模型执行器测试")
class VolcanoExecutorTest {

    private static final String TEST_DATA_DIR = "test_data_volcano_executor";

    private BufferPool bufferPool;
    private InnoDBStorageEngine storageEngine;
    private VolcanoExecutor executor;
    private SQLParser parser;

    @BeforeEach
    void setUp() {
        // 清理测试数据
        cleanupTestData();

        // 创建BufferPool
        bufferPool = new BufferPool(10);

        // 创建StorageEngine
        storageEngine = new InnoDBStorageEngine(10, false);

        // 创建Executor
        executor = new VolcanoExecutor(storageEngine);

        // 创建Parser
        parser = new SQLParser();

        // 创建表: users(id INT, name VARCHAR(100), age INT, salary DOUBLE)
        List<Column> columns = Arrays.asList(
                new Column("id", DataType.INT, false),
                new Column("name", DataType.VARCHAR, 100, true),
                new Column("age", DataType.INT, false),
                new Column("salary", DataType.DOUBLE, false)
        );

        storageEngine.createTable("users", columns);

        // 获取表对象并插入测试数据
        Table table = storageEngine.getTable("users");
        table.insertRow(new Row(table.getColumns(), new Object[]{1, "Alice", 25, 5000.0}));
        table.insertRow(new Row(table.getColumns(), new Object[]{2, "Bob", 17, 3000.0}));
        table.insertRow(new Row(table.getColumns(), new Object[]{3, "Charlie", 30, 6000.0}));
        table.insertRow(new Row(table.getColumns(), new Object[]{4, "David", 15, 2000.0}));
        table.insertRow(new Row(table.getColumns(), new Object[]{5, "Eve", 35, 7000.0}));
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
    @DisplayName("测试执行SELECT * FROM users")
    void testExecuteSelectAll() throws Exception {
        String sql = "SELECT * FROM users";
        Statement stmt = parser.parse(sql);

        QueryResult result = executor.execute(stmt);

        // 验证列定义
        assertEquals(4, result.getColumnCount());
        assertEquals("id", result.getColumns().get(0).getName());
        assertEquals("name", result.getColumns().get(1).getName());
        assertEquals("age", result.getColumns().get(2).getName());
        assertEquals("salary", result.getColumns().get(3).getName());

        // 验证行数
        assertEquals(5, result.getRowCount());

        // 验证行数据
        List<Row> rows = result.getRows();
        assertTrue(rows.stream().anyMatch(r -> r.getValue("name").equals("Alice")));
        assertTrue(rows.stream().anyMatch(r -> r.getValue("name").equals("Bob")));
        assertTrue(rows.stream().anyMatch(r -> r.getValue("name").equals("Charlie")));
        assertTrue(rows.stream().anyMatch(r -> r.getValue("name").equals("David")));
        assertTrue(rows.stream().anyMatch(r -> r.getValue("name").equals("Eve")));
    }

    @Test
    @DisplayName("测试执行SELECT id, name FROM users")
    void testExecuteSelectSpecificColumns() throws Exception {
        String sql = "SELECT id, name FROM users";
        Statement stmt = parser.parse(sql);

        QueryResult result = executor.execute(stmt);

        // 验证列定义
        assertEquals(2, result.getColumnCount());
        assertEquals("id", result.getColumns().get(0).getName());
        assertEquals("name", result.getColumns().get(1).getName());

        // 验证行数
        assertEquals(5, result.getRowCount());

        // 验证行数据
        List<Row> rows = result.getRows();
        for (Row row : rows) {
            assertEquals(2, row.getColumnCount());
            assertNotNull(row.getValue(0)); // id
            assertNotNull(row.getValue(1)); // name
        }
    }

    @Test
    @DisplayName("测试执行SELECT * FROM users WHERE age > 18")
    void testExecuteSelectWithWhere() throws Exception {
        String sql = "SELECT * FROM users WHERE age > 18";
        Statement stmt = parser.parse(sql);

        QueryResult result = executor.execute(stmt);

        // 验证行数(应该返回3行: Alice 25, Charlie 30, Eve 35)
        assertEquals(3, result.getRowCount());

        // 验证所有行的age都 > 18
        List<Row> rows = result.getRows();
        for (Row row : rows) {
            assertTrue((Integer) row.getValue("age") > 18);
        }

        // 验证具体数据
        assertTrue(rows.stream().anyMatch(r -> r.getValue("name").equals("Alice")));
        assertTrue(rows.stream().anyMatch(r -> r.getValue("name").equals("Charlie")));
        assertTrue(rows.stream().anyMatch(r -> r.getValue("name").equals("Eve")));
        assertFalse(rows.stream().anyMatch(r -> r.getValue("name").equals("Bob")));
        assertFalse(rows.stream().anyMatch(r -> r.getValue("name").equals("David")));
    }

    @Test
    @DisplayName("测试执行SELECT id, name FROM users WHERE age > 18 AND age < 30")
    void testExecuteSelectWithComplexWhere() throws Exception {
        String sql = "SELECT id, name FROM users WHERE age > 18 AND age < 30";
        Statement stmt = parser.parse(sql);

        QueryResult result = executor.execute(stmt);

        // 验证行数(应该只返回1行: Alice 25)
        assertEquals(1, result.getRowCount());

        // 验证列定义
        assertEquals(2, result.getColumnCount());

        // 验证数据
        Row row = result.getRows().get(0);
        assertEquals(1, row.getValue(0));
        assertEquals("Alice", row.getValue(1));
    }

    @Test
    @DisplayName("测试执行SELECT name FROM users WHERE age = 35")
    void testExecuteSelectWithEquals() throws Exception {
        String sql = "SELECT name FROM users WHERE age = 35";
        Statement stmt = parser.parse(sql);

        QueryResult result = executor.execute(stmt);

        // 验证行数(应该只返回1行: Eve)
        assertEquals(1, result.getRowCount());

        // 验证数据
        Row row = result.getRows().get(0);
        assertEquals("Eve", row.getValue(0));
    }

    @Test
    @DisplayName("测试执行SELECT * FROM users WHERE age < 18 OR age > 30")
    void testExecuteSelectWithOrCondition() throws Exception {
        String sql = "SELECT * FROM users WHERE age < 18 OR age > 30";
        Statement stmt = parser.parse(sql);

        QueryResult result = executor.execute(stmt);

        // 验证行数(应该返回3行: Bob 17, David 15, Eve 35)
        assertEquals(3, result.getRowCount());

        // 验证数据
        List<Row> rows = result.getRows();
        assertTrue(rows.stream().anyMatch(r -> r.getValue("name").equals("Bob")));
        assertTrue(rows.stream().anyMatch(r -> r.getValue("name").equals("David")));
        assertTrue(rows.stream().anyMatch(r -> r.getValue("name").equals("Eve")));
    }

    @Test
    @DisplayName("测试查询不存在的表抛异常")
    void testExecuteSelectNonExistentTable() throws Exception {
        String sql = "SELECT * FROM unknown_table";
        Statement stmt = parser.parse(sql);

        assertThrows(VolcanoExecutor.ExecutionException.class, () -> {
            executor.execute(stmt);
        });
    }

    @Test
    @DisplayName("测试执行非SELECT语句抛异常")
    void testExecuteNonSelectStatement() throws Exception {
        String sql = "CREATE TABLE test (id INT)";
        Statement stmt = parser.parse(sql);

        assertThrows(UnsupportedOperationException.class, () -> {
            executor.execute(stmt);
        });
    }

    @Test
    @DisplayName("测试execute()空指针检查")
    void testExecuteNullStatement() {
        assertThrows(IllegalArgumentException.class, () -> {
            executor.execute(null);
        });
    }

    @Test
    @DisplayName("测试QueryResult格式化输出")
    void testQueryResultToString() throws Exception {
        String sql = "SELECT id, name FROM users WHERE age > 30";
        Statement stmt = parser.parse(sql);

        QueryResult result = executor.execute(stmt);

        String output = result.toString();

        // 验证输出包含表头和数据
        assertNotNull(output);
        assertTrue(output.contains("id"));
        assertTrue(output.contains("name"));
        assertTrue(output.contains("Eve"));
        assertTrue(output.contains("1 row"));
    }

    @Test
    @DisplayName("测试空结果集的格式化输出")
    void testEmptyResultSetToString() throws Exception {
        String sql = "SELECT * FROM users WHERE age > 100";
        Statement stmt = parser.parse(sql);

        QueryResult result = executor.execute(stmt);

        String output = result.toString();

        // 验证输出为空结果集提示
        assertTrue(output.contains("Empty set"));
    }

    @Test
    @DisplayName("测试VolcanoExecutor构造函数空指针检查")
    void testConstructorNullCheck() {
        assertThrows(IllegalArgumentException.class, () -> {
            new VolcanoExecutor(null);
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
