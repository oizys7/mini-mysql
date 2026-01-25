package com.minimysql.executor;

import com.minimysql.CommonConstant;
import com.minimysql.executor.operator.*;
import com.minimysql.parser.SQLParser;
import com.minimysql.parser.Statement;
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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * ExecutionPlanTest - 查询计划生成器测试
 *
 * 测试ExecutionPlan的功能:
 * - SELECT语句生成 Scan → Filter → Project 链
 * - INSERT语句生成 InsertOperator
 * - UPDATE语句生成 UpdateOperator
 * - DELETE语句生成 DeleteOperator
 * - CREATE TABLE语句生成 CreateTableOperator
 * - DROP TABLE语句生成 DropTableOperator
 */
@DisplayName("查询计划生成器测试")
class ExecutionPlanTest {

    private static final String TEST_DATA_DIR = CommonConstant.DATA_PREFIX + "test_data_execution_plan";

    private StorageEngine storageEngine;
    private SQLParser parser;

    @BeforeEach
    void setUp() {
        // 创建StorageEngine (不使用元数据持久化)
        storageEngine = StorageEngineFactory.createEngine(
                StorageEngineFactory.EngineType.INNODB,
                10,
                false,
                TEST_DATA_DIR
        );

        // 创建SQL解析器
        parser = new SQLParser();
    }

    @AfterEach
    void tearDown() {
        // 关闭StorageEngine
        if (storageEngine != null) {
            storageEngine.close();
        }
    }

    @Test
    @DisplayName("测试SELECT *语句生成ScanOperator")
    void testBuildSelectAll() throws Exception {
        // 准备数据: 创建表并插入数据
        createTestTable();
        insertTestData();

        // 解析SQL
        Statement stmt = parser.parse("SELECT * FROM users");

        // 构建执行计划
        Operator plan = ExecutionPlan.build(stmt, storageEngine);

        // 验证: SELECT * 不需要ProjectOperator,直接是ScanOperator
        assertInstanceOf(ScanOperator.class, plan);

        // 执行计划
        int rowCount = 0;
        while (plan.hasNext()) {
            Row row = plan.next();
            assertNotNull(row);
            rowCount++;
        }

        // 验证: 应该返回3行数据
        assertEquals(3, rowCount);
    }

    @Test
    @DisplayName("测试SELECT列名语句生成Scan → Project链")
    void testBuildSelectWithColumns() throws Exception {
        // 准备数据
        createTestTable();
        insertTestData();

        // 解析SQL
        Statement stmt = parser.parse("SELECT name, age FROM users");

        // 构建执行计划
        Operator plan = ExecutionPlan.build(stmt, storageEngine);

        // 验证: 应该是ProjectOperator
        assertInstanceOf(ProjectOperator.class, plan);

        // 执行计划
        int rowCount = 0;
        while (plan.hasNext()) {
            Row row = plan.next();
            assertNotNull(row);
            rowCount++;
        }

        // 验证: 应该返回3行数据
        assertEquals(3, rowCount);
    }

    @Test
    @DisplayName("测试SELECT WHERE语句生成Scan → Filter → Project链")
    void testBuildSelectWithWhere() throws Exception {
        // 准备数据
        createTestTable();
        insertTestData();

        // 解析SQL
        Statement stmt = parser.parse("SELECT * FROM users WHERE age > 25");

        // 构建执行计划
        Operator plan = ExecutionPlan.build(stmt, storageEngine);

        // 验证: 应该是FilterOperator
        assertInstanceOf(FilterOperator.class, plan);

        // 执行计划
        int rowCount = 0;
        while (plan.hasNext()) {
            Row row = plan.next();
            assertNotNull(row);
            rowCount++;
        }

        // 验证: 应该返回2行数据(Bob和Charlie)
        assertEquals(2, rowCount);
    }

    @Test
    @DisplayName("测试INSERT语句生成InsertOperator")
    void testBuildInsert() throws Exception {
        // 准备数据
        createTestTable();

        // 解析SQL
        Statement stmt = parser.parse("INSERT INTO users VALUES (4, 'David', 40)");

        // 构建执行计划
        Operator plan = ExecutionPlan.build(stmt, storageEngine);

        // 验证: 应该是InsertOperator
        assertInstanceOf(InsertOperator.class, plan);

        // 执行计划
        InsertOperator insertOp = (InsertOperator) plan;
        int affectedRows = insertOp.execute();

        // 验证: 应该插入1行
        assertEquals(1, affectedRows);

        // 验证: 数据确实插入了
        Table table = storageEngine.getTable("users");
        Row row = table.selectByPrimaryKey(4);
        assertNotNull(row);
        assertEquals("David", row.getValue("name"));
        assertEquals(40, row.getValue(0));
    }

    @Test
    @DisplayName("测试UPDATE语句生成UpdateOperator")
    void testBuildUpdate() throws Exception {
        // 准备数据
        createTestTable();
        insertTestData();

        // 解析SQL
        Statement stmt = parser.parse("UPDATE users SET age = 26 WHERE id = 1");

        // 构建执行计划
        Operator plan = ExecutionPlan.build(stmt, storageEngine);

        // 验证: 应该是UpdateOperator
        assertInstanceOf(UpdateOperator.class, plan);

        // 执行计划
        UpdateOperator updateOp = (UpdateOperator) plan;
        int affectedRows = updateOp.execute();

        // 验证: 应该更新1行
        assertEquals(1, affectedRows);

        // 验证: 数据确实更新了
        Table table = storageEngine.getTable("users");
        Row row = table.selectByPrimaryKey(1);
        assertEquals(26, row.getValue(0));
    }

    @Test
    @DisplayName("测试DELETE语句生成DeleteOperator")
    void testBuildDelete() throws Exception {
        // 准备数据
        createTestTable();
        insertTestData();

        // 解析SQL
        Statement stmt = parser.parse("DELETE FROM users WHERE id = 1");

        // 构建执行计划
        Operator plan = ExecutionPlan.build(stmt, storageEngine);

        // 验证: 应该是DeleteOperator
        assertInstanceOf(DeleteOperator.class, plan);

        // 执行计划
        DeleteOperator deleteOp = (DeleteOperator) plan;
        int affectedRows = deleteOp.execute();

        // 验证: 应该删除1行
        assertEquals(1, affectedRows);

        // 验证: 数据确实删除了
        Table table = storageEngine.getTable("users");
        Row row = table.selectByPrimaryKey(1);
        assertNull(row);
    }

    @Test
    @DisplayName("测试CREATE TABLE语句生成CreateTableOperator")
    void testBuildCreateTable() throws Exception {
        // 解析SQL
        Statement stmt = parser.parse("CREATE TABLE products (id INT, name VARCHAR(100), price INT)");

        // 构建执行计划
        Operator plan = ExecutionPlan.build(stmt, storageEngine);

        // 验证: 应该是CreateTableOperator
        assertInstanceOf(CreateTableOperator.class, plan);

        // 执行计划
        CreateTableOperator createOp = (CreateTableOperator) plan;
        Table table = createOp.execute();

        // 验证: 表创建成功
        assertNotNull(table);
        assertEquals("products", table.getTableName());
        assertEquals(3, table.getColumnCount());

        // 验证: 表可以在StorageEngine中找到
        Table found = storageEngine.getTable("products");
        assertNotNull(found);
    }

    @Test
    @DisplayName("测试DROP TABLE语句生成DropTableOperator")
    void testBuildDropTable() throws Exception {
        // 准备数据: 创建表
        createTestTable();

        // 验证: 表存在
        assertNotNull(storageEngine.getTable("users"));

        // 解析SQL
        Statement stmt = parser.parse("DROP TABLE users");

        // 构建执行计划
        Operator plan = ExecutionPlan.build(stmt, storageEngine);

        // 验证: 应该是DropTableOperator
        assertInstanceOf(DropTableOperator.class, plan);

        // 执行计划
        DropTableOperator dropOp = (DropTableOperator) plan;
        boolean success = dropOp.execute();

        // 验证: 删除成功
        assertTrue(success);

        // 验证: 表不存在了
        assertNull(storageEngine.getTable("users"));
    }

    @Test
    @DisplayName("测试表名不存在抛出异常")
    void testTableNotFound() throws Exception {
        // 解析SQL: 查询不存在的表
        Statement stmt = parser.parse("SELECT * FROM not_exist_table");

        // 构建执行计划应该抛出异常
        assertThrows(IllegalArgumentException.class, () -> {
            ExecutionPlan.build(stmt, storageEngine);
        });
    }

    @Test
    @DisplayName("测试null参数检查")
    void testNullParameters() {
        // Statement为null
        assertThrows(IllegalArgumentException.class, () -> {
            ExecutionPlan.build(null, storageEngine);
        });

        // StorageEngine为null
        assertThrows(IllegalArgumentException.class, () -> {
            Statement stmt = new Statement() {
                @Override
                public StatementType getType() {
                    return StatementType.SELECT;
                }
            };
            ExecutionPlan.build(stmt, null);
        });
    }

    /**
     * 创建测试表
     */
    private void createTestTable() {
        List<Column> columns = Arrays.asList(
                new Column("id", DataType.INT, false),
                new Column("name", DataType.VARCHAR, 100, true),
                new Column("age", DataType.INT, false)
        );

        storageEngine.createTable("users", columns);
    }

    /**
     * 插入测试数据
     */
    private void insertTestData() {
        Table table = storageEngine.getTable("users");

        table.insertRow(new Row(new Object[]{1, "Alice", 25}));
        table.insertRow(new Row(new Object[]{2, "Bob", 30}));
        table.insertRow(new Row(new Object[]{3, "Charlie", 35}));
    }
}
