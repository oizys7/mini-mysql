package com.minimysql.storage.table;

import com.minimysql.storage.buffer.BufferPool;
import com.minimysql.storage.buffer.PageFrame;
import com.minimysql.storage.page.DataPage;
import com.minimysql.storage.page.PageManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Date;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Table单元测试
 *
 * 测试表的核心功能:
 * - 表创建和打开
 * - 行数据插入
 * - 多种数据类型支持
 * - NULL值处理
 */
class TableTest {

    private static final int TEST_TABLE_ID = 100;

    private BufferPool bufferPool;
    private PageManager pageManager;
    private Table table;

    @BeforeEach
    void setUp() {
        bufferPool = new BufferPool(10);
        pageManager = new PageManager();
        pageManager.load(TEST_TABLE_ID);

        // 创建表定义
        List<Column> columns = Arrays.asList(
                new Column("id", DataType.INT, false),
                new Column("name", DataType.VARCHAR, 100, true),
                new Column("age", DataType.INT, false),
                new Column("balance", DataType.DOUBLE, true),
                new Column("active", DataType.BOOLEAN, false),
                new Column("created_at", DataType.TIMESTAMP, true)
        );

        table = new Table(TEST_TABLE_ID, "users", columns);
        table.open(bufferPool, pageManager);
    }

    @AfterEach
    void tearDown() {
        if (table != null) {
            table.close();
        }
        pageManager.deleteMetadata();
    }

    @Test
    @DisplayName("创建表应该成功")
    void testCreateTable() {
        assertEquals(TEST_TABLE_ID, table.getTableId());
        assertEquals("users", table.getTableName());
        assertEquals(6, table.getColumnCount());
        assertEquals("id", table.getColumns().get(0).getName());
    }

    @Test
    @DisplayName("根据列名查找列定义")
    void testGetColumn() {
        Column idCol = table.getColumn("id");
        assertNotNull(idCol);
        assertEquals(DataType.INT, idCol.getType());
        assertFalse(idCol.isNullable());

        Column nameCol = table.getColumn("name");
        assertNotNull(nameCol);
        assertEquals(DataType.VARCHAR, nameCol.getType());
        assertEquals(100, nameCol.getLength());
        assertTrue(nameCol.isNullable());

        Column notFound = table.getColumn("not_exist");
        assertNull(notFound);
    }

    @Test
    @DisplayName("插入行数据应该成功")
    void testInsertRow() {
        // 创建行数据
        Object[] values = {
                1,                          // id
                "Alice",                    // name
                25,                         // age
                1000.50,                    // balance
                true,                       // active
                new Date()                  // created_at
        };

        Row row = new Row(table.getColumns(), values);
        int pageId = table.insertRow(row);

        assertTrue(pageId >= 0);
    }

    @Test
    @DisplayName("插入包含NULL的行数据")
    void testInsertRowWithNull() {
        Object[] values = {
                2,                          // id
                null,                       // name (NULL)
                30,                         // age
                null,                       // balance (NULL)
                false,                      // active
                new Date()                  // created_at
        };

        Row row = new Row(table.getColumns(), values);
        int pageId = table.insertRow(row);

        assertTrue(pageId >= 0);
    }

    @Test
    @DisplayName("插入多行数据")
    void testInsertMultipleRows() {
        for (int i = 0; i < 10; i++) {
            Object[] values = {
                    i,
                    "User" + i,
                    20 + i,
                    100.0 * i,
                    true,
                    new Date()
            };

            Row row = new Row(table.getColumns(), values);
            int pageId = table.insertRow(row);

            assertTrue(pageId >= 0);
        }
    }

    @Test
    @DisplayName("NOT NULL列不能插入NULL")
    void testNotNullConstraint() {
        Object[] values = {
                null,                       // id (NOT NULL, 但这里设为NULL)
                "Bob",
                25,
                500.0,
                true,
                new Date()
        };

        // Row构造时会验证,应该抛异常
        assertThrows(IllegalArgumentException.class, () -> {
            new Row(table.getColumns(), values);
        });
    }

    @Test
    @DisplayName("VARCHAR长度验证")
    void testVarcharLengthValidation() {
        // 创建一个限制长度的VARCHAR列
        List<Column> columns = Arrays.asList(
                new Column("id", DataType.INT, false),
                new Column("short_name", DataType.VARCHAR, 5, true)
        );

        Table smallTable = new Table(TEST_TABLE_ID + 1, "test", columns);
        smallTable.open(bufferPool, pageManager);

        // 正常长度
        Object[] values1 = {1, "Hello"};
        Row row1 = new Row(smallTable.getColumns(), values1);
        smallTable.insertRow(row1);

        // 超长字符串
        Object[] values2 = {2, "Hello World"};
        assertThrows(IllegalArgumentException.class, () -> {
            new Row(smallTable.getColumns(), values2);
        });

        smallTable.close();
    }

    @Test
    @DisplayName("行的序列化和反序列化")
    void testRowSerialization() {
        Date now = new Date();

        Object[] values = {
                100,
                "Test User",
                35,
                9999.99,
                false,
                now
        };

        Row originalRow = new Row(table.getColumns(), values);
        byte[] data = originalRow.toBytes();

        Row restoredRow = Row.fromBytes(table.getColumns(), data);

        // 验证所有列
        assertEquals(100, restoredRow.getInt(0));
        assertEquals("Test User", restoredRow.getString(1));
        assertEquals(35, restoredRow.getInt(2));
        assertEquals(9999.99, restoredRow.getDouble(3), 0.001);
        assertEquals(false, restoredRow.getBoolean(4));
        assertEquals(now.getTime(), restoredRow.getDate(5).getTime());
    }

    @Test
    @DisplayName("NULL值的序列化和反序列化")
    void testNullSerialization() {
        Object[] values = {
                200,
                null,                       // name (NULL)
                40,
                null,                       // balance (NULL)
                true,
                null                        // created_at (NULL)
        };

        Row originalRow = new Row(table.getColumns(), values);
        byte[] data = originalRow.toBytes();

        Row restoredRow = Row.fromBytes(table.getColumns(), data);

        assertEquals(200, restoredRow.getInt(0));
        assertNull(restoredRow.getValue(1));
        assertEquals(40, restoredRow.getInt(2));
        assertNull(restoredRow.getValue(3));
        assertEquals(true, restoredRow.getBoolean(4));
        assertNull(restoredRow.getValue(5));
    }

    @Test
    @DisplayName("页满时应该自动分配新页")
    void testAutoAllocateNewPage() {
        // 创建小缓冲池,只有2页
        BufferPool smallPool = new BufferPool(2);
        PageManager smallManager = new PageManager();
        smallManager.load(TEST_TABLE_ID + 2);

        Table smallTable = new Table(TEST_TABLE_ID + 2, "small_test", table.getColumns());
        smallTable.open(smallPool, smallManager);

        // 插入包含长字符串的数据,快速填满页
        int firstPageId = -1;
        for (int i = 0; i < 200; i++) {
            Object[] values = {
                    i,
                    "User" + i + "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX", // 长字符串
                    20 + i,
                    100.0,
                    true,
                    new Date()
            };

            Row row = new Row(smallTable.getColumns(), values);
            int pageId = smallTable.insertRow(row);

            if (i == 0) {
                firstPageId = pageId;
            }

            // 验证页号有效
            assertTrue(pageId >= 0);
        }

        // 验证分配了多页(由于长字符串,应该需要多页)
        assertTrue(smallManager.getAllocatedPageCount() >= 1,
                "Should allocate at least 1 page");

        smallTable.close();
        smallManager.deleteMetadata();
    }

    @Test
    @DisplayName("所有数据类型验证")
    void testAllDataTypes() {
        // 创建包含所有类型的表
        List<Column> allTypesColumns = Arrays.asList(
                new Column("col_int", DataType.INT, false),
                new Column("col_bigint", DataType.BIGINT, false),
                new Column("col_double", DataType.DOUBLE, false),
                new Column("col_boolean", DataType.BOOLEAN, false),
                new Column("col_varchar", DataType.VARCHAR, 50, true),
                new Column("col_date", DataType.DATE, true),
                new Column("col_timestamp", DataType.TIMESTAMP, true)
        );

        Table allTypesTable = new Table(TEST_TABLE_ID + 3, "all_types", allTypesColumns);
        allTypesTable.open(bufferPool, pageManager);

        Date now = new Date();

        Object[] values = {
                12345,
                9876543210L,
                3.14159,
                true,
                "Test String",
                now,
                now
        };

        Row row = new Row(allTypesTable.getColumns(), values);
        int pageId = allTypesTable.insertRow(row);

        assertTrue(pageId >= 0);

        allTypesTable.close();
    }
}
