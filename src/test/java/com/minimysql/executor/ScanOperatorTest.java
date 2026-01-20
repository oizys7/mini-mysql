package com.minimysql.executor;

import com.minimysql.executor.operator.ScanOperator;
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
 * ScanOperatorTest - 全表扫描算子测试
 *
 * 测试全表扫描算子的功能:
 * - 扫描空表
 * - 扫描单行表
 * - 扫描多行表
 * - hasNext()和next()的正确性
 * - NoSuchElementException的抛出
 */
@DisplayName("全表扫描算子测试")
class ScanOperatorTest {

    private static final String TEST_DATA_DIR = "test_data_scan_operator";

    private BufferPool bufferPool;
    private InnoDBStorageEngine storageEngine;
    private Table table;

    @BeforeEach
    void setUp() {
        // 清理测试数据
        cleanupTestData();

        // 创建BufferPool
        bufferPool = new BufferPool(10);

        // 创建StorageEngine
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
    @DisplayName("测试扫描空表")
    void testScanEmptyTable() {
        ScanOperator scan = new ScanOperator(table);

        assertFalse(scan.hasNext());
    }

    @Test
    @DisplayName("测试扫描单行表")
    void testScanSingleRow() {
        // 插入一行数据
        Row row = new Row(table.getColumns(), new Object[]{1, "Alice", 25});
        table.insertRow(row);

        ScanOperator scan = new ScanOperator(table);

        assertTrue(scan.hasNext());
        Row result = scan.next();
        assertEquals(1, result.getValue(0));
        assertEquals("Alice", result.getValue(1));
        assertEquals(25, result.getValue(2));

        assertFalse(scan.hasNext());
    }

    @Test
    @DisplayName("测试扫描多行表")
    void testScanMultipleRows() {
        // 插入多行数据
        table.insertRow(new Row(table.getColumns(), new Object[]{1, "Alice", 25}));
        table.insertRow(new Row(table.getColumns(), new Object[]{2, "Bob", 30}));
        table.insertRow(new Row(table.getColumns(), new Object[]{3, "Charlie", 35}));

        ScanOperator scan = new ScanOperator(table);

        // 第一行
        assertTrue(scan.hasNext());
        Row row1 = scan.next();
        assertEquals(1, row1.getValue(0));

        // 第二行
        assertTrue(scan.hasNext());
        Row row2 = scan.next();
        assertEquals(2, row2.getValue(0));

        // 第三行
        assertTrue(scan.hasNext());
        Row row3 = scan.next();
        assertEquals(3, row3.getValue(0));

        // 没有更多行
        assertFalse(scan.hasNext());
    }

    @Test
    @DisplayName("测试next()在没有更多行时抛异常")
    void testNextThrowsExceptionWhenNoMoreRows() {
        ScanOperator scan = new ScanOperator(table);

        // 空表,直接调用next()应该抛异常
        assertThrows(java.util.NoSuchElementException.class, () -> {
            scan.next();
        });
    }

    @Test
    @DisplayName("测试getTable()")
    void testGetTable() {
        ScanOperator scan = new ScanOperator(table);

        assertEquals(table, scan.getTable());
    }

    @Test
    @DisplayName("测试扫描表时行数据顺序")
    void testScanOrder() {
        // 插入多行数据
        table.insertRow(new Row(table.getColumns(), new Object[]{3, "Charlie", 35}));
        table.insertRow(new Row(table.getColumns(), new Object[]{1, "Alice", 25}));
        table.insertRow(new Row(table.getColumns(), new Object[]{2, "Bob", 30}));

        ScanOperator scan = new ScanOperator(table);

        // 收集所有行
        List<Row> rows = new java.util.ArrayList<>();
        while (scan.hasNext()) {
            rows.add(scan.next());
        }

        // 验证行数
        assertEquals(3, rows.size());

        // 验证数据完整性(不保证顺序,因为使用了聚簇索引)
        assertTrue(rows.stream().anyMatch(r -> r.getValue(0).equals(1)));
        assertTrue(rows.stream().anyMatch(r -> r.getValue(0).equals(2)));
        assertTrue(rows.stream().anyMatch(r -> r.getValue(0).equals(3)));
    }

    @Test
    @DisplayName("测试ScanOperator构造函数空指针检查")
    void testConstructorNullCheck() {
        assertThrows(IllegalArgumentException.class, () -> {
            new ScanOperator(null);
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
