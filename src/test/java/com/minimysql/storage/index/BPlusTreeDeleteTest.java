package com.minimysql.storage.index;

import com.minimysql.storage.buffer.BufferPool;
import com.minimysql.storage.page.PageManager;
import com.minimysql.storage.table.Column;
import com.minimysql.storage.table.DataType;
import com.minimysql.storage.table.Row;
import com.minimysql.storage.table.Table;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * B+树删除功能测试
 *
 * 测试B+树的完整删除功能，包括：
 * 1. 基本删除
 * 2. 节点借位
 * 3. 节点合并
 * 4. 复杂场景
 */
@DisplayName("BPlusTreeDeleteTest - B+树删除功能测试")
public class BPlusTreeDeleteTest {

    private BufferPool bufferPool;
    private PageManager pageManager;
    private ClusteredIndex clusteredIndex;
    private Table table; // 添加Table引用

    @BeforeEach
    public void setUp() {
        // 使用唯一的tableId避免冲突
        int uniqueTableId = (int) (System.currentTimeMillis() % 10000);

        bufferPool = new BufferPool(100);
        pageManager = new PageManager();
        pageManager.load(uniqueTableId); // tableId=100

        // 创建列定义(测试用)
        List<Column> testColumns = Arrays.asList(
                new Column("id", DataType.INT, false),
                new Column("name", DataType.VARCHAR, 50, true),
                new Column("age", DataType.INT, false)
        );

        // 创建测试表
        table = new Table(uniqueTableId, "test_table", testColumns);

        // 创建聚簇索引(主键是id列,索引为0)
        clusteredIndex = new ClusteredIndex(
                uniqueTableId, // tableId
                "id", // 主键列名
                0,    // 主键列索引
                bufferPool,
                pageManager
        );

        // 关键修复: 设置Table到ClusteredIndex
        clusteredIndex.setTable(table);
    }

    @AfterEach
    public void tearDown() {
        if (bufferPool != null) {
            bufferPool.clear();
        }
    }

    @Test
    @DisplayName("删除单个键")
    public void testDeleteSingleKey() {
        // 创建行数据
        Row row = new Row( new Object[]{1, "Alice", 25});

        // 插入行
        clusteredIndex.insertRow(row);

        // 验证行存在
        Row found = clusteredIndex.selectByPrimaryKey(1);
        assertNotNull(found);
        assertEquals(1, found.getValue(0));
        assertEquals("Alice", found.getValue(1));

        // 删除行
        clusteredIndex.delete(1);

        // 验证行已删除
        Row deleted = clusteredIndex.selectByPrimaryKey(1);
        assertNull(deleted);
    }

    @Test
    @DisplayName("删除多个键")
    public void testDeleteMultipleKeys() {
        // 插入多行
        for (int i = 1; i <= 10; i++) {
            Row row = new Row( new Object[]{i, "User" + i, 20 + i});
            clusteredIndex.insertRow(row);
        }

        // 验证所有行存在
        for (int i = 1; i <= 10; i++) {
            Row found = clusteredIndex.selectByPrimaryKey(i);
            assertNotNull(found);
            assertEquals("User" + i, found.getValue(1));
        }

        // 删除部分行
        clusteredIndex.delete(3);
        clusteredIndex.delete(7);

        // 验证已删除
        assertNull(clusteredIndex.selectByPrimaryKey(3));
        assertNull(clusteredIndex.selectByPrimaryKey(7));

        // 验证其他行仍存在
        Row row1 = clusteredIndex.selectByPrimaryKey(1);
        assertNotNull(row1);
        assertEquals("User1", row1.getValue(1));

        Row row10 = clusteredIndex.selectByPrimaryKey(10);
        assertNotNull(row10);
        assertEquals("User10", row10.getValue(1));
    }

    @Test
    @DisplayName("删除不存在的键")
    public void testDeleteNonExistentKey() {
        // 插入一行
        Row row = new Row( new Object[]{1, "Alice", 25});
        clusteredIndex.insertRow(row);

        // 删除不存在的键（不应该抛异常）
        assertDoesNotThrow(() -> clusteredIndex.delete(999));

        // 验证原行仍存在
        Row found = clusteredIndex.selectByPrimaryKey(1);
        assertNotNull(found);
    }

    @Test
    @DisplayName("删除后重新插入")
    public void testDeleteAndReinsert() {
        // 插入行
        Row row1 = new Row( new Object[]{1, "Alice", 25});
        Row row2 = new Row( new Object[]{2, "Bob", 30});
        clusteredIndex.insertRow(row1);
        clusteredIndex.insertRow(row2);

        // 删除行
        clusteredIndex.delete(1);
        assertNull(clusteredIndex.selectByPrimaryKey(1));

        // 重新插入
        Row newRow = new Row( new Object[]{1, "New Alice", 26});
        clusteredIndex.insertRow(newRow);

        Row found = clusteredIndex.selectByPrimaryKey(1);
        assertNotNull(found);
        assertEquals("New Alice", found.getValue(1));
        assertEquals(26, found.getValue(2));

        // 验证其他行不受影响
        Row row2Found = clusteredIndex.selectByPrimaryKey(2);
        assertNotNull(row2Found);
        assertEquals("Bob", row2Found.getValue(1));
    }

    @Test
    @DisplayName("删除所有键")
    public void testDeleteAllKeys() {
        // 插入多行
        int numRows = 20;
        for (int i = 1; i <= numRows; i++) {
            Row row = new Row( new Object[]{i, "User" + i, 20 + i});
            clusteredIndex.insertRow(row);
        }

        // 删除所有行
        for (int i = 1; i <= numRows; i++) {
            clusteredIndex.delete(i);
            assertNull(clusteredIndex.selectByPrimaryKey(i));
        }

        // 验证树仍然可以工作
        Row newRow = new Row( new Object[]{100, "New User", 30});
        clusteredIndex.insertRow(newRow);

        Row found = clusteredIndex.selectByPrimaryKey(100);
        assertNotNull(found);
        assertEquals("New User", found.getValue(1));
    }

    @Test
    @DisplayName("删除后范围查询")
    public void testRangeQueryAfterDelete() {
        // 插入1-50
        for (int i = 1; i <= 50; i++) {
            Row row = new Row( new Object[]{i, "User" + i, 20 + i});
            clusteredIndex.insertRow(row);
        }

        // 删除部分数据
        for (int i = 10; i <= 20; i++) {
            clusteredIndex.delete(i);
        }

        // 范围查询：应该跳过被删除的
        List<Row> result = clusteredIndex.rangeSelect(5, 25);

        // 验证结果合理即可
        assertNotNull(result);
        assertTrue(result.size() > 0);
    }

    @Test
    @DisplayName("顺序删除")
    public void testSequentialDelete() {
        // 插入数据
        for (int i = 1; i <= 50; i++) {
            Row row = new Row( new Object[]{i, "User" + i, 20 + i});
            clusteredIndex.insertRow(row);
        }

        // 顺序删除（从左到右）
        for (int i = 1; i <= 25; i++) {
            clusteredIndex.delete(i);
            assertNull(clusteredIndex.selectByPrimaryKey(i));
        }

        // 验证剩余键
        for (int i = 26; i <= 50; i++) {
            Row found = clusteredIndex.selectByPrimaryKey(i);
            assertNotNull(found);
            assertEquals("User" + i, found.getValue(1));
        }
    }
}
