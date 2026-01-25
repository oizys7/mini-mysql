package com.minimysql.storage.impl;

import com.minimysql.storage.StorageEngine;
import com.minimysql.storage.table.Column;
import com.minimysql.storage.table.DataType;
import com.minimysql.storage.table.Row;
import com.minimysql.storage.table.Table;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * InnoDBStorageEngineTest - InnoDB存储引擎测试
 *
 * 测试InnoDBStorageEngine的核心功能:
 * 1. 创建表
 * 2. 获取表
 * 3. 删除表
 * 4. 插入行数据
 * 5. 关闭引擎
 */
@DisplayName("InnoDBStorageEngineTest - InnoDB存储引擎测试")
class InnoDBStorageEngineTest {

    private StorageEngine engine;

    /**
     * 数据目录路径
     */
    private static final String DATA_DIR = "data";

    @BeforeEach
    void setUp() {
        // 创建InnoDB存储引擎(缓冲池大小:100页，禁用元数据持久化以便测试)
        // 测试环境不需要持久化元数据，避免系统表干扰测试结果
        engine = new InnoDBStorageEngine(100, false);
    }

    @AfterEach
    void tearDown() throws IOException {
        // 关闭引擎
        if (engine != null) {
            engine.close();
        }

        // 清理测试数据目录
        Path dataPath = Paths.get(DATA_DIR);
        if (Files.exists(dataPath)) {
            Files.walk(dataPath)
                    .sorted((a, b) -> b.compareTo(a)) // 逆序删除(先删除文件再删除目录)
                    .forEach(path -> {
                        try {
                            Files.delete(path);
                        } catch (IOException e) {
                            // 忽略删除失败
                        }
                    });
        }
    }

    /**
     * 测试创建表
     */
    @Test
    @DisplayName("创建表")
    void testCreateTable() {
        // 创建列定义
        List<Column> columns = Arrays.asList(
                new Column("id", DataType.INT, false),
                new Column("name", DataType.VARCHAR, 100, true),
                new Column("age", DataType.INT, false)
        );

        // 创建表
        Table users = engine.createTable("users", columns);

        // 验证表创建成功
        assertNotNull(users, "Table should not be null");
        assertEquals("users", users.getTableName());
        assertEquals(3, users.getColumnCount());
        assertEquals(1, users.getTableId()); // 第一个表的ID为1

        // 验证表已注册到引擎
        assertTrue(engine.tableExists("users"));
        assertEquals(1, engine.getTableCount());
    }

    /**
     * 测试创建重复表名
     */
    @Test
    @DisplayName("创建重复表名应抛出异常")
    void testCreateDuplicateTable() {
        List<Column> columns = Arrays.asList(
                new Column("id", DataType.INT, false)
        );

        // 创建第一个表
        engine.createTable("users", columns);

        // 尝试创建同名的表，应该抛出异常
        assertThrows(IllegalArgumentException.class, () -> {
            engine.createTable("users", columns);
        });
    }

    /**
     * 测试创建空表名
     */
    @Test
    @DisplayName("创建空表名应抛出异常")
    void testCreateTableWithEmptyName() {
        List<Column> columns = Arrays.asList(
                new Column("id", DataType.INT, false)
        );

        // 空表名应该抛出异常
        assertThrows(IllegalArgumentException.class, () -> {
            engine.createTable("", columns);
        });

        // null表名应该抛出异常
        assertThrows(IllegalArgumentException.class, () -> {
            engine.createTable(null, columns);
        });
    }

    /**
     * 测试创建空列列表
     */
    @Test
    @DisplayName("创建空列列表应抛出异常")
    void testCreateTableWithEmptyColumns() {
        // 空列列表应该抛出异常
        assertThrows(IllegalArgumentException.class, () -> {
            engine.createTable("test", List.of());
        });

        // null列列表应该抛出异常
        assertThrows(IllegalArgumentException.class, () -> {
            engine.createTable("test", null);
        });
    }

    /**
     * 测试获取表
     */
    @Test
    @DisplayName("获取表")
    void testGetTable() {
        List<Column> columns = Arrays.asList(
                new Column("id", DataType.INT, false)
        );

        // 创建表
        Table createdTable = engine.createTable("users", columns);

        // 获取表
        Table retrievedTable = engine.getTable("users");

        // 验证是同一个实例
        assertSame(createdTable, retrievedTable);
        assertEquals("users", retrievedTable.getTableName());
    }

    /**
     * 测试获取不存在的表
     */
    @Test
    @DisplayName("获取不存在的表应返回null")
    void testGetNonExistentTable() {
        Table table = engine.getTable("nonexistent");
        assertNull(table);
    }

    /**
     * 测试删除表
     */
    @Test
    @DisplayName("删除表")
    void testDropTable() {
        List<Column> columns = Arrays.asList(
                new Column("id", DataType.INT, false)
        );

        // 创建表
        engine.createTable("users", columns);
        assertTrue(engine.tableExists("users"));

        // 删除表
        boolean dropped = engine.dropTable("users");
        assertTrue(dropped);
        assertFalse(engine.tableExists("users"));
        assertEquals(0, engine.getTableCount());
    }

    /**
     * 测试删除不存在的表
     */
    @Test
    @DisplayName("删除不存在的表应返回false")
    void testDropNonExistentTable() {
        boolean dropped = engine.dropTable("nonexistent");
        assertFalse(dropped);
    }

    /**
     * 测试获取所有表名
     */
    @Test
    @DisplayName("获取所有表名")
    void testGetAllTableNames() {
        List<Column> columns = Arrays.asList(
                new Column("id", DataType.INT, false)
        );

        // 创建多个表
        engine.createTable("users", columns);
        engine.createTable("orders", columns);
        engine.createTable("products", columns);

        // 获取所有表名
        List<String> tableNames = engine.getAllTableNames();

        // 验证表名数量
        assertEquals(3, tableNames.size());

        // 验证包含所有表名
        assertTrue(tableNames.contains("users"));
        assertTrue(tableNames.contains("orders"));
        assertTrue(tableNames.contains("products"));
    }

    /**
     * 测试表ID自动递增
     */
    @Test
    @DisplayName("表ID自动递增")
    void testTableIdAutoIncrement() {
        List<Column> columns = Arrays.asList(
                new Column("id", DataType.INT, false)
        );

        // 创建多个表
        Table table1 = engine.createTable("table1", columns);
        Table table2 = engine.createTable("table2", columns);
        Table table3 = engine.createTable("table3", columns);

        // 验证表ID递增
        assertEquals(1, table1.getTableId());
        assertEquals(2, table2.getTableId());
        assertEquals(3, table3.getTableId());
    }

    /**
     * 测试插入行数据
     */
    @Test
    @DisplayName("插入行数据")
    void testInsertRow() {
        // 创建表
        List<Column> columns = Arrays.asList(
                new Column("id", DataType.INT, false),
                new Column("name", DataType.VARCHAR, 100, true)
        );

        Table users = engine.createTable("users", columns);

        // 插入行
        Object[] values1 = {1, "Alice"};
        Row row1 = new Row(values1);
        int pageId1 = users.insertRow(row1);
        assertTrue(pageId1 >= 0);

        // 插入另一行
        Object[] values2 = {2, "Bob"};
        Row row2 = new Row(values2);
        int pageId2 = users.insertRow(row2);
        assertTrue(pageId2 >= 0);
    }

    /**
     * 测试关闭引擎
     */
    @Test
    @DisplayName("关闭引擎")
    void testCloseEngine() {
        List<Column> columns = Arrays.asList(
                new Column("id", DataType.INT, false)
        );

        // 创建表
        engine.createTable("users", columns);
        engine.createTable("orders", columns);

        // 验证表数量
        assertEquals(2, engine.getTableCount());

        // 关闭引擎
        engine.close();

        // 验证引擎已关闭
        assertTrue(((InnoDBStorageEngine) engine).isClosed());

        // 验证关闭后无法创建表
        assertThrows(IllegalStateException.class, () -> {
            engine.createTable("new_table", columns);
        });
    }

    /**
     * 测试重置引擎
     */
    @Test
    @DisplayName("重置引擎")
    void testResetEngine() {
        List<Column> columns = Arrays.asList(
                new Column("id", DataType.INT, false)
        );

        // 创建表
        engine.createTable("users", columns);
        engine.createTable("orders", columns);

        // 重置引擎
        ((InnoDBStorageEngine) engine).reset();

        // 验证表已清空
        assertEquals(0, engine.getTableCount());
        assertFalse(engine.tableExists("users"));
        assertFalse(engine.tableExists("orders"));

        // 验证可以重新创建表(表ID从1开始)
        Table newTable = engine.createTable("new_table", columns);
        assertEquals(1, newTable.getTableId());
    }

    /**
     * 测试获取缓冲池
     */
    @Test
    @DisplayName("获取缓冲池")
    void testGetBufferPool() {
        InnoDBStorageEngine innodbEngine = (InnoDBStorageEngine) engine;
        assertNotNull(innodbEngine.getBufferPool());
        assertEquals(100, innodbEngine.getBufferPool().getPoolSize());
    }

    /**
     * 测试toString方法
     */
    @Test
    @DisplayName("测试toString方法")
    void testToString() {
        String str = engine.toString();
        assertNotNull(str);
        assertTrue(str.contains("InnoDBStorageEngine"));
        assertTrue(str.contains("bufferPoolSize=100"));
        assertTrue(str.contains("tableCount=0"));
        assertTrue(str.contains("closed=false"));
    }

    /**
     * 测试多个表的独立性
     */
    @Test
    @DisplayName("测试多个表的独立性")
    void testMultipleTablesIndependence() {
        // 创建两个表
        List<Column> userColumns = Arrays.asList(
                new Column("id", DataType.INT, false),
                new Column("name", DataType.VARCHAR, 100, true)
        );

        List<Column> orderColumns = Arrays.asList(
                new Column("id", DataType.INT, false),
                new Column("user_id", DataType.INT, false)
        );

        Table users = engine.createTable("users", userColumns);
        Table orders = engine.createTable("orders", orderColumns);

        // 验证表独立
        assertNotSame(users, orders);
        assertEquals("users", users.getTableName());
        assertEquals("orders", orders.getTableName());
        assertNotEquals(users.getTableId(), orders.getTableId());

        // 验证列独立
        assertEquals(2, users.getColumnCount());
        assertEquals(2, orders.getColumnCount());
        assertNotNull(users.getColumn("name"));
        assertNull(orders.getColumn("name"));
        assertNotNull(orders.getColumn("user_id"));
        assertNull(users.getColumn("user_id"));
    }

    /**
     * 测试策略模式：可以切换不同的存储引擎实现
     */
    @Test
    @DisplayName("测试策略模式：切换不同存储引擎")
    void testStrategyPattern() {
        // 验证可以使用接口类型引用
        StorageEngine engine1 = new InnoDBStorageEngine(100);
        StorageEngine engine2 = new InnoDBStorageEngine(200);

        // 两个独立的引擎实例
        assertNotSame(engine1, engine2);

        // 可以正常使用
        List<Column> columns = Arrays.asList(
                new Column("id", DataType.INT, false)
        );

        Table table1 = engine1.createTable("table1", columns);
        Table table2 = engine2.createTable("table2", columns);

        assertNotNull(table1);
        assertNotNull(table2);

        // 清理
        engine1.close();
        engine2.close();
    }

    /**
     * 测试关闭后无法操作表
     */
    @Test
    @DisplayName("关闭后无法操作表")
    void testCannotOperateAfterClose() {
        List<Column> columns = Arrays.asList(
                new Column("id", DataType.INT, false)
        );

        // 创建表
        engine.createTable("users", columns);

        // 关闭引擎
        engine.close();

        // 验证无法获取表
        assertThrows(IllegalStateException.class, () -> {
            engine.getTable("users");
        });

        // 验证无法检查表是否存在
        assertThrows(IllegalStateException.class, () -> {
            engine.tableExists("users");
        });

        // 验证无法获取所有表名
        assertThrows(IllegalStateException.class, () -> {
            engine.getAllTableNames();
        });

        // 验证无法获取表数量
        assertThrows(IllegalStateException.class, () -> {
            engine.getTableCount();
        });
    }
}
