package com.minimysql.storage;

import com.minimysql.storage.impl.InnoDBStorageEngine;
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
 * IndexManagementTest - 索引管理和CRUD测试
 *
 * 测试以下功能:
 * 1. 创建二级索引 (createIndex)
 * 2. 删除二级索引 (dropIndex)
 * 3. 更新行数据 (updateRow)
 * 4. 删除行数据 (deleteRow)
 */
@DisplayName("IndexManagementTest - 索引管理和CRUD测试")
class IndexManagementTest {

    private InnoDBStorageEngine storageEngine;
    private static final String DATA_DIR = "data";

    @BeforeEach
    void setUp() {
        storageEngine = new InnoDBStorageEngine(100);
    }

    @AfterEach
    void tearDown() throws IOException {
        if (storageEngine != null) {
            storageEngine.close();
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
     * 测试创建二级索引
     */
    @Test
    @DisplayName("创建二级索引")
    void testCreateSecondaryIndex() {
        // 创建表
        List<Column> columns = Arrays.asList(
                new Column("id", DataType.INT, false),
                new Column("name", DataType.VARCHAR, 50, true)
        );
        Table users = storageEngine.createTable("users", columns);

        // 验证初始状态:没有二级索引
        assertEquals(0, users.getSecondaryIndexCount());

        // 创建二级索引
        storageEngine.createIndex("users", "idx_name", "name", false);

        // 验证索引已创建
        assertEquals(1, users.getSecondaryIndexCount());
        assertNotNull(users.getSecondaryIndex("idx_name"));
        assertEquals("name", users.getSecondaryIndex("idx_name").getColumnName());
        assertFalse(users.getSecondaryIndex("idx_name").isUnique());
    }

    /**
     * 测试创建唯一索引
     */
    @Test
    @DisplayName("创建唯一索引")
    void testCreateUniqueIndex() {
        // 创建表
        List<Column> columns = Arrays.asList(
                new Column("id", DataType.INT, false),
                new Column("email", DataType.VARCHAR, 100, true)
        );
        Table users = storageEngine.createTable("users", columns);

        // 创建唯一索引
        storageEngine.createIndex("users", "idx_email", "email", true);

        // 验证索引属性
        assertTrue(users.getSecondaryIndex("idx_email").isUnique());
    }

    /**
     * 测试创建重复索引抛出异常
     */
    @Test
    @DisplayName("创建重复索引抛异常")
    void testCreateDuplicateIndexThrowsException() {
        // 创建表
        List<Column> columns = Arrays.asList(
                new Column("id", DataType.INT, false),
                new Column("name", DataType.VARCHAR, 50, true)
        );
        storageEngine.createTable("users", columns);

        // 创建第一个索引
        storageEngine.createIndex("users", "idx_name", "name", false);

        // 尝试创建重复索引,应该抛出异常
        assertThrows(IllegalArgumentException.class, () -> {
            storageEngine.createIndex("users", "idx_name", "name", false);
        });
    }

    /**
     * 测试在不存在的表上创建索引抛出异常
     */
    @Test
    @DisplayName("不存在的表上创建索引抛异常")
    void testCreateIndexOnNonExistentTableThrowsException() {
        assertThrows(IllegalArgumentException.class, () -> {
            storageEngine.createIndex("nonexistent", "idx_name", "name", false);
        });
    }

    /**
     * 测试在不存在的列上创建索引抛出异常
     */
    @Test
    @DisplayName("不存在的列上创建索引抛异常")
    void testCreateIndexOnNonExistentColumnThrowsException() {
        // 创建表
        List<Column> columns = Arrays.asList(
                new Column("id", DataType.INT, false)
        );
        storageEngine.createTable("users", columns);

        // 在不存在的列上创建索引,应该抛出异常
        assertThrows(IllegalArgumentException.class, () -> {
            storageEngine.createIndex("users", "idx_name", "name", false);
        });
    }

    /**
     * 测试删除二级索引
     */
    @Test
    @DisplayName("删除二级索引")
    void testDropSecondaryIndex() {
        // 创建表和索引
        List<Column> columns = Arrays.asList(
                new Column("id", DataType.INT, false),
                new Column("name", DataType.VARCHAR, 50, true)
        );
        storageEngine.createTable("users", columns);
        storageEngine.createIndex("users", "idx_name", "name", false);

        // 验证索引存在
        assertEquals(1, storageEngine.getTable("users").getSecondaryIndexCount());

        // 删除索引
        boolean result = storageEngine.dropIndex("users", "idx_name");

        // 验证删除成功
        assertTrue(result);
        assertEquals(0, storageEngine.getTable("users").getSecondaryIndexCount());
        assertNull(storageEngine.getTable("users").getSecondaryIndex("idx_name"));
    }

    /**
     * 测试删除不存在的索引返回false
     */
    @Test
    @DisplayName("删除不存在的索引返回false")
    void testDropNonExistentIndexReturnsFalse() {
        // 创建表
        List<Column> columns = Arrays.asList(
                new Column("id", DataType.INT, false)
        );
        storageEngine.createTable("users", columns);

        // 删除不存在的索引
        boolean result = storageEngine.dropIndex("users", "idx_name");

        // 验证返回false
        assertFalse(result);
    }

    /**
     * 测试删除聚簇索引抛出异常
     */
    @Test
    @DisplayName("删除聚簇索引抛异常")
    void testDropClusteredIndexThrowsException() {
        // 创建表
        List<Column> columns = Arrays.asList(
                new Column("id", DataType.INT, false)
        );
        storageEngine.createTable("users", columns);

        // 尝试删除聚簇索引,应该抛出异常
        assertThrows(UnsupportedOperationException.class, () -> {
            storageEngine.dropIndex("users", "PRIMARY");
        });
    }

    /**
     * 测试更新行数据
     */
    @Test
    @DisplayName("更新行数据")
    void testUpdateRow() {
        // 创建表
        List<Column> columns = Arrays.asList(
                new Column("id", DataType.INT, false),
                new Column("name", DataType.VARCHAR, 50, true)
        );
        Table users = storageEngine.createTable("users", columns);

        // 插入数据
        Object[] values1 = {1, "Alice"};
        Row row1 = new Row(values1);
        users.insertRow(row1);

        Object[] values2 = {2, "Bob"};
        Row row2 = new Row(values2);
        users.insertRow(row2);

        // 验证初始数据
        Row original = users.selectByPrimaryKey(1);
        assertEquals("Alice", original.getValue(1));

        // 更新数据
        Object[] updatedValues = {1, "Alice Updated"};
        Row updatedRow = new Row(updatedValues);
        int affected = users.updateRow(1, updatedRow);

        // 验证更新成功
        assertEquals(1, affected);
        Row updated = users.selectByPrimaryKey(1);
        assertEquals("Alice Updated", updated.getValue(1));

        // 验证其他数据未受影响
        Row unchanged = users.selectByPrimaryKey(2);
        assertEquals("Bob", unchanged.getValue(1));
    }

    /**
     * 测试更新不存在的行返回0
     */
    @Test
    @DisplayName("更新不存在的行返回0")
    void testUpdateNonExistentRowReturns0() {
        // 创建表
        List<Column> columns = Arrays.asList(
                new Column("id", DataType.INT, false),
                new Column("name", DataType.VARCHAR, 50, true)
        );
        Table users = storageEngine.createTable("users", columns);

        // 更新不存在的行
        Object[] values = {999, "Nonexistent"};
        Row row = new Row(values);
        int affected = users.updateRow(999, row);

        // 验证返回0
        assertEquals(0, affected);
    }

    /**
     * 测试删除行数据
     */
    @Test
    @DisplayName("删除行数据")
    void testDeleteRow() {
        // 创建表
        List<Column> columns = Arrays.asList(
                new Column("id", DataType.INT, false),
                new Column("name", DataType.VARCHAR, 50, true)
        );
        Table users = storageEngine.createTable("users", columns);

        // 插入数据
        Object[] values1 = {1, "Alice"};
        Row row1 = new Row(values1);
        users.insertRow(row1);

        Object[] values2 = {2, "Bob"};
        Row row2 = new Row(values2);
        users.insertRow(row2);

        // 验证数据存在
        assertNotNull(users.selectByPrimaryKey(1));
        assertNotNull(users.selectByPrimaryKey(2));

        // 删除一行
        int affected = users.deleteRow(1);

        // 验证删除成功
        assertEquals(1, affected);
        assertNull(users.selectByPrimaryKey(1));
        assertNotNull(users.selectByPrimaryKey(2));
    }

    /**
     * 测试删除不存在的行返回0
     */
    @Test
    @DisplayName("删除不存在的行返回0")
    void testDeleteNonExistentRowReturns0() {
        // 创建表
        List<Column> columns = Arrays.asList(
                new Column("id", DataType.INT, false)
        );
        Table users = storageEngine.createTable("users", columns);

        // 删除不存在的行
        int affected = users.deleteRow(999);

        // 验证返回0
        assertEquals(0, affected);
    }

    /**
     * 测试完整的CRUD操作
     */
    @Test
    @DisplayName("完整CRUD操作")
    void testFullCRUD() {
        // 创建表
        List<Column> columns = Arrays.asList(
                new Column("id", DataType.INT, false),
                new Column("name", DataType.VARCHAR, 50, true),
                new Column("age", DataType.INT, false)
        );
        Table users = storageEngine.createTable("users", columns);

        // CREATE: 插入数据
        Object[] values = {1, "Alice", 25};
        Row row = new Row(values);
        users.insertRow(row);

        // READ: 查询数据
        Row inserted = users.selectByPrimaryKey(1);
        assertNotNull(inserted);
        assertEquals("Alice", inserted.getValue(1));
        assertEquals(25, inserted.getValue(2));

        // UPDATE: 更新数据
        Object[] updatedValues = {1, "Alice Updated", 26};
        Row updated = new Row(updatedValues);
        users.updateRow(1, updated);

        Row afterUpdate = users.selectByPrimaryKey(1);
        assertEquals("Alice Updated", afterUpdate.getValue(1));
        assertEquals(26, afterUpdate.getValue(2));

        // DELETE: 删除数据
        users.deleteRow(1);
        assertNull(users.selectByPrimaryKey(1));
    }
}
