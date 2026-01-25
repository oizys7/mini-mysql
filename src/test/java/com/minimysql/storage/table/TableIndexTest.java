package com.minimysql.storage.table;

import com.minimysql.storage.impl.InnoDBStorageEngine;
import com.minimysql.storage.StorageEngine;
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
 * TableIndexTest - 表索引功能测试
 *
 * 测试Table的索引管理和查询功能:
 * 1. 聚簇索引的创建和使用
 * 2. 主键查询
 * 3. 插入时索引同步更新
 * 4. 范围查询
 */
@DisplayName("Table - 聚簇索引管理测试")
class TableIndexTest {

    private StorageEngine engine;

    private static final String DATA_DIR = "data";

    @BeforeEach
    void setUp() {
        engine = new InnoDBStorageEngine(100);
    }

    @AfterEach
    void tearDown() throws IOException {
        if (engine != null) {
            engine.close();
        }

        // 清理测试数据目录
        Path dataPath = Paths.get(DATA_DIR);
        if (Files.exists(dataPath)) {
            Files.walk(dataPath)
                    .sorted((a, b) -> b.compareTo(a))
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
     * 测试聚簇索引是否正确设置到Table
     */
    @Test
    @DisplayName("聚簇索引设置")
    void testClusteredIndexSet() {
        // 创建表
        List<Column> columns = Arrays.asList(
                new Column("id", DataType.INT, false),
                new Column("name", DataType.VARCHAR, 100, true)
        );

        Table users = engine.createTable("users", columns);

        // 验证聚簇索引已设置
        assertNotNull(users.getClusteredIndex(), "Clustered index should be set");
        assertEquals("PRIMARY", users.getClusteredIndex().getIndexName());
        assertEquals("id", users.getClusteredIndex().getPrimaryKeyColumn());
    }

    /**
     * 测试主键查询
     */
    @Test
    @DisplayName("主键查询")
    void testSelectByPrimaryKey() {
        // 创建表
        List<Column> columns = Arrays.asList(
                new Column("id", DataType.INT, false),
                new Column("name", DataType.VARCHAR, 100, true)
        );

        Table users = engine.createTable("users", columns);

        // 插入数据
        Object[] values1 = {1, "Alice"};
        Row row1 = new Row(values1);
        users.insertRow(row1);

        Object[] values2 = {2, "Bob"};
        Row row2 = new Row(values2);
        users.insertRow(row2);

        // 主键查询
        Row found = users.selectByPrimaryKey(1);
        assertNotNull(found, "Should find row with id=1");
        assertEquals(1, found.getValue(0));
        assertEquals("Alice", found.getValue(1));

        // 查询不存在的主键
        Row notFound = users.selectByPrimaryKey(999);
        assertNull(notFound, "Should not find row with id=999");
    }

    /**
     * 测试插入时索引同步更新
     */
    @Test
    @DisplayName("插入时更新索引")
    void testInsertUpdatesIndexes() {
        // 创建表
        List<Column> columns = Arrays.asList(
                new Column("id", DataType.INT, false),
                new Column("name", DataType.VARCHAR, 100, true)
        );

        Table users = engine.createTable("users", columns);

        // 插入多行
        for (int i = 1; i <= 10; i++) {
            Object[] values = {i, "User" + i};
            Row row = new Row(values);
            users.insertRow(row);
        }

        // 验证所有行都可以通过主键查询到
        for (int i = 1; i <= 10; i++) {
            Row found = users.selectByPrimaryKey(i);
            assertNotNull(found, "Should find row with id=" + i);
            assertEquals(i, found.getValue(0));
            assertEquals("User" + i, found.getValue(1));
        }
    }

    /**
     * 测试范围查询
     */
    @Test
    @DisplayName("范围查询")
    void testRangeSelect() {
        // 创建表
        List<Column> columns = Arrays.asList(
                new Column("id", DataType.INT, false),
                new Column("name", DataType.VARCHAR, 100, true)
        );

        Table users = engine.createTable("users", columns);

        // 插入数据（ID: 1, 2, 3, 4, 5）
        for (int i = 1; i <= 5; i++) {
            Object[] values = {i, "User" + i};
            Row row = new Row(values);
            users.insertRow(row);
        }

        // 范围查询 [2, 4]
        List<Row> results = users.rangeSelect(2, 4);

        assertNotNull(results);
        assertEquals(3, results.size(), "Should find 3 rows in range [2, 4]");

        // 验证结果
        assertEquals(2, results.get(0).getValue(0));
        assertEquals(3, results.get(1).getValue(0));
        assertEquals(4, results.get(2).getValue(0));
    }

    /**
     * 测试二级索引管理
     */
    @Test
    @DisplayName("二级索引管理")
    void testSecondaryIndexManagement() {
        // 创建表
        List<Column> columns = Arrays.asList(
                new Column("id", DataType.INT, false),
                new Column("username", DataType.VARCHAR, 100, true),
                new Column("email", DataType.VARCHAR, 100, true)
        );

        Table users = engine.createTable("users", columns);

        // 验证初始没有二级索引
        assertEquals(0, users.getSecondaryIndexCount());
        assertTrue(users.getSecondaryIndexNames().isEmpty());

        // TODO 注意: 当前不支持通过API创建二级索引
        // 二级索引需要在InnoDBStorageEngine中添加createIndex方法
        // 这里只是验证数据结构已就绪
    }

    /**
     * 测试查询时聚簇索引未设置
     */
    @Test
    @DisplayName("没有聚簇索引时的查询")
    void testQueryWithoutClusteredIndex() {
        // 创建没有聚簇索引的表（手动创建Table，不通过StorageEngine）
        List<Column> columns = Arrays.asList(
                new Column("id", DataType.INT, false),
                new Column("name", DataType.VARCHAR, 100, true)
        );

        Table table = new Table(999, "test_table", columns);
        // 不设置聚簇索引

        // 尝试主键查询应该抛出异常
        assertThrows(IllegalStateException.class, () -> {
            table.selectByPrimaryKey(1);
        });

        // 尝试范围查询应该抛出异常
        assertThrows(IllegalStateException.class, () -> {
            table.rangeSelect(1, 10);
        });
    }

    /**
     * 测试索引名称和列名
     */
    @Test
    @DisplayName("索引元数据")
    void testIndexMetadata() {
        // 创建表
        List<Column> columns = Arrays.asList(
                new Column("user_id", DataType.INT, false),
                new Column("username", DataType.VARCHAR, 100, true)
        );

        Table users = engine.createTable("users", columns);

        // 验证聚簇索引元数据
        assertEquals("PRIMARY", users.getClusteredIndex().getIndexName());
        assertEquals("user_id", users.getClusteredIndex().getPrimaryKeyColumn());
        assertEquals(0, users.getClusteredIndex().getPrimaryKeyIndex());
    }

    /**
     * 测试多表独立性
     */
    @Test
    @DisplayName("多表独立性")
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

        // 插入不同表的数据
        Object[] userValues = {1, "Alice"};
        Row userRow = new Row(userValues);
        users.insertRow(userRow);

        Object[] orderValues = {1, 1};
        Row orderRow = new Row(orderValues);
        orders.insertRow(orderRow);

        // 验证两个表的聚簇索引独立
        assertNotNull(users.getClusteredIndex());
        assertNotNull(orders.getClusteredIndex());

        assertNotEquals(users.getClusteredIndex(), orders.getClusteredIndex());

        // 验证查询结果独立
        Row foundUser = users.selectByPrimaryKey(1);
        assertEquals("Alice", foundUser.getValue(1));

        Row foundOrder = orders.selectByPrimaryKey(1);
        assertEquals(1, foundOrder.getValue(1));
    }

    /**
     * 测试全表扫描（当前是简化实现）
     */
    @Test
    @DisplayName("全表扫描")
    void testFullTableScan() {
        // 创建表
        List<Column> columns = Arrays.asList(
                new Column("id", DataType.INT, false),
                new Column("name", DataType.VARCHAR, 100, true)
        );

        Table users = engine.createTable("users", columns);

        // 插入数据
        Object[] values = {1, "Alice"};
        Row row = new Row(values);
        users.insertRow(row);

        // 全表扫描（通过聚簇索引的getAll()实现）
        List<Row> results = users.fullTableScan();
        assertEquals(1, results.size());

        assertEquals("Alice", users.getRowValue(results.get(0), "name"));
    }
}
