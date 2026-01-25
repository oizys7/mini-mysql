package com.minimysql.integration;

import com.minimysql.CommonConstant;
import com.minimysql.executor.ExecutionPlan;
import com.minimysql.executor.operator.CreateTableOperator;
import com.minimysql.executor.Operator;
import com.minimysql.parser.SQLParser;
import com.minimysql.parser.Statement;
import com.minimysql.storage.StorageEngine;
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
 * MetadataPersistenceTest - 元数据持久化和加载测试
 *
 * 测试元数据的持久化和加载功能:
 * - 创建表后，重启存储引擎能加载表定义
 * - 插入的数据重启后仍然存在
 * - 系统表不受重启影响
 */
@DisplayName("元数据持久化和加载测试")
class MetadataPersistenceTest {

    private static final String TEST_DATA_DIR = getTestDataDir();

    private StorageEngine storageEngine;
    private SQLParser parser;

    /**
     * 获取测试数据目录的绝对路径
     */
    private static String getTestDataDir() {
        String workingDir = System.getProperty("user.dir");
        return workingDir + CommonConstant.DATA_PREFIX + "/test_data_metadata_persistence";
    }

    @BeforeEach
    void setUp() {
        // 清理测试数据
        cleanupTestData();

        // 创建StorageEngine (启用元数据持久化，使用测试专用数据目录)
        storageEngine = new InnoDBStorageEngine(10, true, TEST_DATA_DIR);

        // 创建SQL解析器
        parser = new SQLParser();
    }

    @AfterEach
    void tearDown() {
        // 关闭StorageEngine
        if (storageEngine != null) {
            storageEngine.close();
        }

        // 注意：不在这里清理测试数据，因为有些测试需要验证重启后的数据
        // 测试数据会在setUp()的下一轮测试开始前被清理
    }

    @Test
    @DisplayName("测试创建表后重启能加载表定义")
    void testCreateTableAndReload() throws Exception {
        // 第一步：创建表
        Statement createStmt = parser.parse("CREATE TABLE users (id INT, name VARCHAR(100), age INT)");
        Operator createPlan = ExecutionPlan.build(createStmt, storageEngine);
        CreateTableOperator createOp = (CreateTableOperator) createPlan;
        Table table1 = createOp.execute();

        assertNotNull(table1);
        assertEquals("users", table1.getTableName());
        assertEquals(3, table1.getColumnCount());

        // 第二步：插入数据
        table1.insertRow(new Row(new Object[]{1, "Alice", 25}));
        table1.insertRow(new Row(new Object[]{2, "Bob", 30}));

        // 第三步：关闭存储引擎，模拟重启
        storageEngine.close();

        // 第四步：重新创建存储引擎（模拟重启，使用相同的测试数据目录）
        StorageEngine newStorageEngine = new InnoDBStorageEngine(10, true, TEST_DATA_DIR);

        try {
            // 第五步：验证表定义已经加载
            Table table2 = newStorageEngine.getTable("users");

            assertNotNull(table2, "Table should be loaded after restart");
            assertEquals("users", table2.getTableName());
            assertEquals(3, table2.getColumnCount());

            // 第六步：验证数据仍然存在
            Row row1 = table2.selectByPrimaryKey(1);
            Row row2 = table2.selectByPrimaryKey(2);

            assertNotNull(row1, "Row 1 should exist after restart");
            assertNotNull(row2, "Row 2 should exist after restart");

            assertEquals(1, row1.getValue(0));
            assertEquals("Alice", row1.getValue(1));
            assertEquals(25, row1.getValue(2));

            assertEquals(2, row2.getValue(0));
            assertEquals("Bob", row2.getValue(1));
            assertEquals(30, row2.getValue(2));

        } finally {
            newStorageEngine.close();
        }
    }

    @Test
    @DisplayName("测试多个表的元数据加载")
    void testMultipleTablesMetadataPersistence() throws Exception {
        // 创建多个表
        storageEngine.createTable("users", Arrays.asList(
                new Column("id", DataType.INT, false),
                new Column("name", DataType.VARCHAR, 100, true)
        ));

        storageEngine.createTable("products", Arrays.asList(
                new Column("id", DataType.INT, false),
                new Column("product_name", DataType.VARCHAR, 200, true),
                new Column("price", DataType.INT, false)
        ));

        // 插入一些数据
        Table users = storageEngine.getTable("users");
        users.insertRow(new Row(new Object[]{1, "Alice"}));

        Table products = storageEngine.getTable("products");
        products.insertRow(new Row(new Object[]{1, "Laptop", 1000}));

        // 重启
        storageEngine.close();

        // 重新创建存储引擎（使用相同的测试数据目录）
        StorageEngine newStorageEngine = new InnoDBStorageEngine(10, true, TEST_DATA_DIR);

        try {
            // 验证两个表都加载了
            Table users2 = newStorageEngine.getTable("users");
            Table products2 = newStorageEngine.getTable("products");

            assertNotNull(users2);
            assertNotNull(products2);

            // 验证数据
            Row userRow = users2.selectByPrimaryKey(1);
            assertEquals("Alice", userRow.getValue("name"));

            Row productRow = products2.selectByPrimaryKey(1);
            assertEquals("Laptop", productRow.getValue("product_name"));
            assertEquals(1000, productRow.getValue("price"));

        } finally {
            newStorageEngine.close();
        }
    }

    @Test
    @DisplayName("测试重启后系统表仍然存在")
    void testSystemTablesPersistAfterRestart() throws Exception {
        // 关闭并重启
        storageEngine.close();

        StorageEngine newStorageEngine = new InnoDBStorageEngine(10, true);

        try {
            // 验证系统表存在
            assertNotNull(newStorageEngine.getTable("SYS_TABLES"));
            assertNotNull(newStorageEngine.getTable("SYS_COLUMNS"));

            // 验证系统表可以查询
            Table sysTables = newStorageEngine.getTable("SYS_TABLES");
            List<Row> rows = sysTables.fullTableScan();

            // 系统表本身不在SYS_TABLES中，所以应该为空或只有普通表
            assertNotNull(rows);

        } finally {
            newStorageEngine.close();
        }
    }

    @Test
    @DisplayName("测试重启后能继续插入和查询")
    void testCanInsertAndQueryAfterRestart() throws Exception {
        // 创建表并插入初始数据
        storageEngine.createTable("users", Arrays.asList(
                new Column("id", DataType.INT, false),
                new Column("name", DataType.VARCHAR, 100, true)
        ));

        Table users1 = storageEngine.getTable("users");
        users1.insertRow(new Row(new Object[]{1, "Alice"}));

        // 重启
        storageEngine.close();

        // 重新创建存储引擎（使用相同的测试数据目录）
        StorageEngine newStorageEngine = new InnoDBStorageEngine(10, true, TEST_DATA_DIR);

        try {
            // 插入新数据
            Table users2 = newStorageEngine.getTable("users");
            users2.insertRow(new Row(new Object[]{2, "Bob"}));

            // 验证两行数据都存在
            Row row1 = users2.selectByPrimaryKey(1);
            Row row2 = users2.selectByPrimaryKey(2);

            assertNotNull(row1);
            assertNotNull(row2);

            assertEquals("Alice", row1.getValue("name"));
            assertEquals("Bob", row2.getValue("name"));

        } finally {
            newStorageEngine.close();
        }
    }

    @Test
    @DisplayName("测试禁用元数据持久化时重启不加载表")
    void testNoPersistenceWithoutFlag() {
        // 创建表（禁用元数据持久化）
        StorageEngine noPersistEngine = new InnoDBStorageEngine(10, false);

        try {
            noPersistEngine.createTable("temp", Arrays.asList(
                    new Column("id", DataType.INT, false)
            ));

            // 表应该存在
            assertNotNull(noPersistEngine.getTable("temp"));

            // 重启（创建新实例）
            StorageEngine newEngine = new InnoDBStorageEngine(10, false);

            try {
                // 表不应该存在（因为没有持久化）
                assertNull(newEngine.getTable("temp"));
            } finally {
                newEngine.close();
            }

        } finally {
            noPersistEngine.close();
        }
    }

    /**
     * 清理测试数据
     */
    private void cleanupTestData() {
        File dir = new File(TEST_DATA_DIR);
        if (dir.exists()) {
            deleteDirectory(dir);
        }
    }

    /**
     * 递归删除目录
     */
    private void deleteDirectory(File dir) {
        File[] files = dir.listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.isDirectory()) {
                    deleteDirectory(file);
                } else {
                    file.delete();
                }
            }
        }
        dir.delete();
    }
}
