package com.minimysql.integration;

import com.minimysql.result.QueryResult;
import com.minimysql.executor.VolcanoExecutor;
import com.minimysql.parser.SQLParser;
import com.minimysql.parser.Statement;
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
 * ExecutorIntegrationTest - Executor层集成测试
 *
 * 端到端测试,验证完整的SQL执行流程:
 * Parser → VolcanoExecutor → Operators → QueryResult
 *
 * 测试场景:
 * - 全表扫描查询
 * - 带WHERE条件的查询
 * - 带投影的查询
 * - 复杂查询(WHERE + 投影)
 * - 空结果集查询
 * - 多表场景(可选)
 *
 * 设计原则:
 * - 端到端测试,验证整个执行流程
 * - 使用真实的SQL字符串,不是硬编码的AST
 * - 验证最终结果的正确性
 */
@DisplayName("Executor层集成测试")
class ExecutorIntegrationTest {

    private static final String TEST_DATA_DIR = "test_data_executor_integration";

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

        // 创建测试表: users(id INT, name VARCHAR(100), age INT, salary DOUBLE)
        createUsersTable();

        // 创建测试表: products(id INT, name VARCHAR(100), price DOUBLE, stock INT)
        createProductsTable();
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
    @DisplayName("集成测试: SELECT * FROM users - 全表扫描")
    void testFullTableScan() throws Exception {
        String sql = "SELECT * FROM users";
        Statement stmt = parser.parse(sql);
        QueryResult result = executor.execute(stmt);

        // 验证结果
        assertEquals(4, result.getColumnCount());
        assertEquals(5, result.getRowCount());

        // 打印结果(便于调试)
        System.out.println("\n" + sql);
        System.out.println(result);
    }

    @Test
    @DisplayName("集成测试: SELECT id, name FROM users - 列投影")
    void testColumnProjection() throws Exception {
        String sql = "SELECT id, name FROM users";
        Statement stmt = parser.parse(sql);
        QueryResult result = executor.execute(stmt);

        // 验证结果
        assertEquals(2, result.getColumnCount());
        assertEquals(5, result.getRowCount());

        // 验证列定义
        assertEquals("id", result.getColumns().get(0).getName());
        assertEquals("name", result.getColumns().get(1).getName());

        // 验证所有行的列数
        for (Row row : result.getRows()) {
            assertEquals(2, row.getColumnCount());
        }

        // 打印结果
        System.out.println("\n" + sql);
        System.out.println(result);
    }

    @Test
    @DisplayName("集成测试: SELECT * FROM users WHERE age > 25 - WHERE过滤")
    void testWhereFilter() throws Exception {
        String sql = "SELECT * FROM users WHERE age > 25";
        Statement stmt = parser.parse(sql);
        QueryResult result = executor.execute(stmt);

        // 验证结果
        assertEquals(4, result.getColumnCount());
        assertEquals(2, result.getRowCount()); // Charlie(30), Eve(35)

        // 验证所有行的age都 > 25
        for (Row row : result.getRows()) {
            assertTrue((Integer) row.getValue("age") > 25);
        }

        // 打印结果
        System.out.println("\n" + sql);
        System.out.println(result);
    }

    @Test
    @DisplayName("集成测试: SELECT id, name FROM users WHERE age > 20 AND age < 35 - 复杂查询")
    void testComplexQuery() throws Exception {
        String sql = "SELECT id, name FROM users WHERE age > 20 AND age < 35";
        Statement stmt = parser.parse(sql);
        QueryResult result = executor.execute(stmt);

        // 验证结果
        assertEquals(2, result.getColumnCount());
        assertEquals(2, result.getRowCount()); // Alice(25), Charlie(30)

        // 验证数据
        assertTrue(result.getRows().stream().anyMatch(r -> r.getValue("name").equals("Alice")));
        assertTrue(result.getRows().stream().anyMatch(r -> r.getValue("name").equals("Charlie")));

        // 打印结果
        System.out.println("\n" + sql);
        System.out.println(result);
    }

    @Test
    @DisplayName("集成测试: SELECT * FROM users WHERE age = 30 - 等值查询")
    void testEqualsQuery() throws Exception {
        String sql = "SELECT * FROM users WHERE age = 30";
        Statement stmt = parser.parse(sql);
        QueryResult result = executor.execute(stmt);

        // 验证结果
        assertEquals(1, result.getRowCount());
        Row row = result.getRows().get(0);
        assertEquals("Charlie", row.getValue("name"));
        assertEquals(30, row.getValue("age"));

        // 打印结果
        System.out.println("\n" + sql);
        System.out.println(result);
    }

    @Test
    @DisplayName("集成测试: SELECT * FROM users WHERE age < 18 OR age > 30 - OR条件")
    void testOrCondition() throws Exception {
        String sql = "SELECT * FROM users WHERE age < 18 OR age > 30";
        Statement stmt = parser.parse(sql);
        QueryResult result = executor.execute(stmt);

        // 验证结果
        assertEquals(3, result.getRowCount()); // Bob(17), David(15), Eve(35)

        // 验证数据
        assertTrue(result.getRows().stream().anyMatch(r -> r.getValue("name").equals("Bob")));
        assertTrue(result.getRows().stream().anyMatch(r -> r.getValue("name").equals("David")));
        assertTrue(result.getRows().stream().anyMatch(r -> r.getValue("name").equals("Eve")));

        // 打印结果
        System.out.println("\n" + sql);
        System.out.println(result);
    }

    @Test
    @DisplayName("集成测试: SELECT name FROM users WHERE age > 100 - 空结果集")
    void testEmptyResultSet() throws Exception {
        String sql = "SELECT name FROM users WHERE age > 100";
        Statement stmt = parser.parse(sql);
        QueryResult result = executor.execute(stmt);

        // 验证结果
        assertEquals(0, result.getRowCount());

        // 验证toString()输出
        String output = result.toString();
        assertTrue(output.contains("Empty set"));

        // 打印结果
        System.out.println("\n" + sql);
        System.out.println(result);
    }

    @Test
    @DisplayName("集成测试: 多表查询 - products表")
    void testMultiTableQuery() throws Exception {
        String sql = "SELECT name, price FROM products WHERE stock > 50";
        Statement stmt = parser.parse(sql);
        QueryResult result = executor.execute(stmt);

        // 验证结果
        assertEquals(2, result.getColumnCount());
        assertEquals(2, result.getRowCount()); // Laptop(100), Keyboard(80)

        // 验证列定义
        assertEquals("name", result.getColumns().get(0).getName());
        assertEquals("price", result.getColumns().get(1).getName());

        // 打印结果
        System.out.println("\n" + sql);
        System.out.println(result);
    }

    @Test
    @DisplayName("集成测试: 复杂表达式 - price * stock > 50000")
    void testComplexExpression() throws Exception {
        // 注意:当前实现不支持表达式投影,这里测试WHERE中的算术运算
        String sql = "SELECT * FROM products WHERE price * stock > 50000";
        Statement stmt = parser.parse(sql);
        QueryResult result = executor.execute(stmt);

        // 验证结果(1000 * 60 = 60000 > 50000)
        assertEquals(1, result.getRowCount());
        Row row = result.getRows().get(0);
        assertEquals("Monitor", row.getValue("name"));

        // 打印结果
        System.out.println("\n" + sql);
        System.out.println(result);
    }

    @Test
    @DisplayName("集成测试: 格式化输出验证")
    void testFormattedOutput() throws Exception {
        String sql = "SELECT id, name, age FROM users WHERE age > 25";
        Statement stmt = parser.parse(sql);
        QueryResult result = executor.execute(stmt);

        String output = result.toString();

        // 验证输出包含关键元素
        assertTrue(output.contains("id"));
        assertTrue(output.contains("name"));
        assertTrue(output.contains("age"));
        assertTrue(output.contains("Charlie"));
        assertTrue(output.contains("Eve"));
        assertTrue(output.contains("2 rows"));

        // 打印结果
        System.out.println("\n" + sql);
        System.out.println(result);
        System.out.println("\nFormatted output verification:");
        System.out.println("✓ Contains column headers");
        System.out.println("✓ Contains data rows");
        System.out.println("✓ Contains row count");
    }

    /**
     * 创建users表并插入测试数据
     */
    private void createUsersTable() {
        List<Column> columns = Arrays.asList(
                new Column("id", DataType.INT, false),
                new Column("name", DataType.VARCHAR, 100, true),
                new Column("age", DataType.INT, false),
                new Column("salary", DataType.DOUBLE, false)
        );

        storageEngine.createTable("users", columns);

        Table table = storageEngine.getTable("users");
        table.insertRow(new Row(table.getColumns(), new Object[]{1, "Alice", 25, 5000.0}));
        table.insertRow(new Row(table.getColumns(), new Object[]{2, "Bob", 17, 3000.0}));
        table.insertRow(new Row(table.getColumns(), new Object[]{3, "Charlie", 30, 6000.0}));
        table.insertRow(new Row(table.getColumns(), new Object[]{4, "David", 15, 2000.0}));
        table.insertRow(new Row(table.getColumns(), new Object[]{5, "Eve", 35, 7000.0}));
    }

    /**
     * 创建products表并插入测试数据
     */
    private void createProductsTable() {
        List<Column> columns = Arrays.asList(
                new Column("id", DataType.INT, false),
                new Column("name", DataType.VARCHAR, 100, true),
                new Column("price", DataType.DOUBLE, false),
                new Column("stock", DataType.INT, false)
        );

        storageEngine.createTable("products", columns);

        Table table = storageEngine.getTable("products");
        table.insertRow(new Row(table.getColumns(), new Object[]{1, "Laptop", 1000.0, 50}));
        table.insertRow(new Row(table.getColumns(), new Object[]{2, "Mouse", 50.0, 30}));
        table.insertRow(new Row(table.getColumns(), new Object[]{3, "Keyboard", 100.0, 80}));
        table.insertRow(new Row(table.getColumns(), new Object[]{4, "Monitor", 1000.0, 60}));
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
