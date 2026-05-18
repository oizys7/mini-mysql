package com.minimysql.integration;

import com.minimysql.executor.*;
import com.minimysql.executor.operator.*;
import com.minimysql.parser.SQLParser;
import com.minimysql.parser.Statement;
import com.minimysql.result.QueryResult;
import com.minimysql.storage.StorageEngine;
import com.minimysql.storage.StorageEngineFactory;
import com.minimysql.storage.table.Row;
import org.junit.jupiter.api.*;

import java.io.File;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Mini MySQL 端到端集成测试")
public class MiniMySQLEndToEndTest {

    private static final String TEST_DATA_DIR = "test_data_e2e";

    private StorageEngine storageEngine;
    private SQLParser parser;
    private VolcanoExecutor executor;

    @BeforeEach
    public void setUp() {
        cleanupTestData();

        storageEngine = StorageEngineFactory.createEngine(
                StorageEngineFactory.EngineType.INNODB,
                50,
                true,
                TEST_DATA_DIR
        );

        parser = new SQLParser();
        executor = new VolcanoExecutor(storageEngine);
    }

    @AfterEach
    public void tearDown() {
        if (storageEngine != null) {
            storageEngine.close();
        }
    }

    // ==================== 测试用例 ====================

    @Test
    @DisplayName("测试1: 创建表并插入单行数据")
    void test01_CreateTableAndInsert() {
        executeDDL("CREATE TABLE users (id INT NOT NULL, name VARCHAR(100), age INT, PRIMARY KEY (id))");

        assertTrue(storageEngine.tableExists("users"));
        assertEquals("users", storageEngine.getTable("users").getTableName());
        assertEquals(3, storageEngine.getTable("users").getColumnCount());

        executeDML("INSERT INTO users VALUES (1, 'Alice', 25)");

        QueryResult result = query("SELECT * FROM users");
        assertEquals(1, result.getRowCount());

        Row row = result.getRows().get(0);
        assertEquals(1, row.getValue(0));
        assertEquals("Alice", row.getValue(1));
        assertEquals(25, row.getValue(2));
    }

    @Test
    @DisplayName("测试2: 插入多行数据并查询")
    void test02_InsertMultipleRows() {
        executeDDL("CREATE TABLE products (id INT NOT NULL, name VARCHAR(100), price INT, PRIMARY KEY (id))");

        executeDML("INSERT INTO products VALUES (1, 'Laptop', 1000)");
        executeDML("INSERT INTO products VALUES (2, 'Mouse', 50)");
        executeDML("INSERT INTO products VALUES (3, 'Keyboard', 150)");

        QueryResult result = query("SELECT * FROM products");
        assertEquals(3, result.getRowCount());
    }

    @Test
    @DisplayName("测试3: WHERE 条件查询")
    void test03_SelectWithWhere() {
        executeDDL("CREATE TABLE students (id INT NOT NULL, name VARCHAR(100), score INT, PRIMARY KEY (id))");

        executeDML("INSERT INTO students VALUES (1, 'Alice', 90)");
        executeDML("INSERT INTO students VALUES (2, 'Bob', 85)");
        executeDML("INSERT INTO students VALUES (3, 'Charlie', 95)");

        // WHERE 过滤
        QueryResult result = query("SELECT * FROM students WHERE score > 90");
        assertEquals(1, result.getRowCount());

        Row row = result.getRows().get(0);
        assertEquals(3, row.getValue(0));
        assertEquals("Charlie", row.getValue(1));
        assertEquals(95, row.getValue(2));
    }

    @Test
    @DisplayName("测试4: 元数据持久化 - 重启后数据依然存在")
    void test04_MetadataPersistence() {
        executeDDL("CREATE TABLE employees (id INT NOT NULL, name VARCHAR(100), department VARCHAR(100), PRIMARY KEY (id))");
        executeDML("INSERT INTO employees VALUES (1, 'Alice', 'Engineering')");
        executeDML("INSERT INTO employees VALUES (2, 'Bob', 'Sales')");

        // 模拟重启
        storageEngine.close();
        storageEngine = StorageEngineFactory.createEngine(
                StorageEngineFactory.EngineType.INNODB,
                50,
                true,
                TEST_DATA_DIR
        );
        executor = new VolcanoExecutor(storageEngine);

        // 验证表和数据持久化
        assertTrue(storageEngine.tableExists("employees"));
        assertEquals(3, storageEngine.getTable("employees").getColumnCount());

        QueryResult result = query("SELECT * FROM employees");
        assertEquals(2, result.getRowCount());
    }

    @Test
    @DisplayName("测试5: 创建多个表并验证独立性")
    void test05_MultipleTables() {
        executeDDL("CREATE TABLE users (id INT NOT NULL, name VARCHAR(100), PRIMARY KEY (id))");
        executeDDL("CREATE TABLE orders (id INT NOT NULL, user_id INT, amount INT, PRIMARY KEY (id))");
        executeDDL("CREATE TABLE products (id INT NOT NULL, name VARCHAR(100), PRIMARY KEY (id))");

        executeDML("INSERT INTO users VALUES (1, 'Alice')");
        executeDML("INSERT INTO orders VALUES (1, 1, 100)");
        executeDML("INSERT INTO products VALUES (1, 'Laptop')");

        assertEquals(1, query("SELECT * FROM users").getRowCount());
        assertEquals(1, query("SELECT * FROM orders").getRowCount());
        assertEquals(1, query("SELECT * FROM products").getRowCount());
    }

    @Test
    @DisplayName("测试6: 各种数据类型")
    void test06_DataTypes() {
        executeDDL("CREATE TABLE types_test (id INT NOT NULL, name VARCHAR(100), age INT, score INT, PRIMARY KEY (id))");

        executeDML("INSERT INTO types_test VALUES (1, 'Alice', 25, 90)");
        executeDML("INSERT INTO types_test VALUES (2, 'Bob', 30, 85)");

        QueryResult result = query("SELECT * FROM types_test");
        assertEquals(2, result.getRowCount());

        Row row = result.getRows().get(0);
        assertEquals(1, row.getValue(0));
        assertEquals("Alice", row.getValue(1));
        assertEquals(25, row.getValue(2));
        assertEquals(90, row.getValue(3));
    }

    // ==================== 辅助方法 ====================

    private void executeDDL(String sql) {
        Statement stmt = parser.parse(sql);
        Operator plan = ExecutionPlan.build(stmt, storageEngine);

        if (plan instanceof CreateTableOperator op) {
            op.execute();
        } else if (plan instanceof DropTableOperator op) {
            op.execute();
        }
    }

    private void executeDML(String sql) {
        Statement stmt = parser.parse(sql);
        Operator plan = ExecutionPlan.build(stmt, storageEngine);

        if (plan instanceof MutationOperator op) {
            op.execute();
        }
    }

    private QueryResult query(String sql) {
        Statement stmt = parser.parse(sql);
        return executor.execute(stmt);
    }

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
