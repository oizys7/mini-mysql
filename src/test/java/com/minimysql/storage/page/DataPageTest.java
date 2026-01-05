package com.minimysql.storage.page;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * DataPage单元测试
 *
 * 测试数据页的核心功能:
 * - 页的序列化和反序列化
 * - 行数据的插入、读取、删除
 * - 自由空间管理
 * - 边界情况处理
 */
class DataPageTest {

    @Test
    @DisplayName("创建新的数据页应该初始化为空")
    void testNewPageInitialization() {
        DataPage page = new DataPage();

        assertEquals(0, page.getRowCount());
        assertEquals(0, page.getValidRowCount());
        // 自由空间应该接近16KB (减去页头11字节)
        assertTrue(page.getFreeSpace() > 16000);
        assertEquals(-1, page.getPageId());
    }

    @Test
    @DisplayName("插入行数据应该成功")
    void testInsertRow() {
        DataPage page = new DataPage();

        byte[] row1 = "Hello, Mini MySQL!".getBytes();
        byte[] row2 = "Second row".getBytes();

        int slot1 = page.insertRow(row1);
        int slot2 = page.insertRow(row2);

        assertEquals(0, slot1);
        assertEquals(1, slot2);
        assertEquals(2, page.getRowCount());
        assertEquals(2, page.getValidRowCount());
    }

    @Test
    @DisplayName("读取行数据应该返回插入时的数据")
    void testGetRow() {
        DataPage page = new DataPage();

        byte[] row1 = "First row".getBytes();
        byte[] row2 = "Second row".getBytes();

        page.insertRow(row1);
        page.insertRow(row2);

        byte[] retrieved1 = page.getRow(0);
        byte[] retrieved2 = page.getRow(1);

        assertArrayEquals(row1, retrieved1);
        assertArrayEquals(row2, retrieved2);
    }

    @Test
    @DisplayName("读取不存在的槽位应该返回null")
    void testGetInvalidRow() {
        DataPage page = new DataPage();

        assertNull(page.getRow(-1));
        assertNull(page.getRow(0));
        assertNull(page.getRow(100));
    }

    @Test
    @DisplayName("删除行数据应该标记槽位为无效")
    void testDeleteRow() {
        DataPage page = new DataPage();

        byte[] row1 = "First row".getBytes();
        byte[] row2 = "Second row".getBytes();

        page.insertRow(row1);
        page.insertRow(row2);

        assertTrue(page.deleteRow(0));
        assertEquals(2, page.getRowCount()); // 总行数不变
        assertEquals(1, page.getValidRowCount()); // 有效行数减少
        assertNull(page.getRow(0));
        assertArrayEquals(row2, page.getRow(1));
    }

    @Test
    @DisplayName("删除不存在的槽位应该返回false")
    void testDeleteInvalidRow() {
        DataPage page = new DataPage();

        assertFalse(page.deleteRow(-1));
        assertFalse(page.deleteRow(0));
        assertFalse(page.deleteRow(100));
    }

    @Test
    @DisplayName("获取所有行应该返回有效行")
    void testGetAllRows() {
        DataPage page = new DataPage();

        byte[] row1 = "Row 1".getBytes();
        byte[] row2 = "Row 2".getBytes();
        byte[] row3 = "Row 3".getBytes();

        page.insertRow(row1);
        page.insertRow(row2);
        page.insertRow(row3);

        page.deleteRow(1); // 删除中间的行

        List<byte[]> rows = page.getAllRows();

        assertEquals(2, rows.size());
        assertArrayEquals(row1, rows.get(0));
        assertArrayEquals(row3, rows.get(1));
    }

    @Test
    @DisplayName("页序列化和反序列化应该保持数据一致性")
    void testSerialization() {
        DataPage page = new DataPage();
        page.setPageId(42);

        byte[] row1 = "Serialized row 1".getBytes();
        byte[] row2 = "Serialized row 2".getBytes();

        page.insertRow(row1);
        page.insertRow(row2);

        // 序列化
        byte[] data = page.toBytes();

        // 反序列化
        DataPage restoredPage = new DataPage();
        restoredPage.fromBytes(data);

        // 验证
        assertEquals(42, restoredPage.getPageId());
        assertEquals(2, restoredPage.getRowCount());
        assertArrayEquals(row1, restoredPage.getRow(0));
        assertArrayEquals(row2, restoredPage.getRow(1));
        assertEquals(page.getFreeSpace(), restoredPage.getFreeSpace());
    }

    @Test
    @DisplayName("页应该有固定的16KB大小")
    void testPageSize() {
        DataPage page = new DataPage();

        byte[] data = page.toBytes();

        assertEquals(Page.PAGE_SIZE, data.length);
    }

    @Test
    @DisplayName("插入大量数据直到页满")
    void testPageFull() {
        DataPage page = new DataPage();

        // 插入100字节的行
        byte[] largeRow = new byte[100];
        for (int i = 0; i < largeRow.length; i++) {
            largeRow[i] = (byte) i;
        }

        int count = 0;
        while (true) {
            try {
                page.insertRow(largeRow);
                count++;
            } catch (IllegalStateException e) {
                break;
            }
        }

        // 验证插入了许多行
        assertTrue(count > 100);

        // 验证页已满
        assertFalse(page.hasFreeSpace(100));
    }

    @Test
    @DisplayName("页类型应该是DATA_PAGE")
    void testPageType() {
        DataPage page = new DataPage();

        assertEquals(Page.PageType.DATA_PAGE, page.getType());
    }

    @Test
    @DisplayName("设置和获取页号")
    void testPageId() {
        DataPage page = new DataPage();

        page.setPageId(12345);

        assertEquals(12345, page.getPageId());
    }

    @Test
    @DisplayName("插入空行数据")
    void testInsertEmptyRow() {
        DataPage page = new DataPage();

        byte[] emptyRow = new byte[0];
        int slot = page.insertRow(emptyRow);

        byte[] retrieved = page.getRow(slot);
        assertArrayEquals(emptyRow, retrieved);
    }

    @Test
    @DisplayName("自由空间计算应该准确")
    void testFreeSpaceCalculation() {
        DataPage page = new DataPage();

        int initialFreeSpace = page.getFreeSpace();

        byte[] row = "Test row data".getBytes();
        page.insertRow(row);

        // 自由空间应该减少(行数据 + 行头 + 槽位)
        int expectedUsedSpace = row.length + 4 + 2; // row + header + slot
        int expectedFreeSpace = initialFreeSpace - expectedUsedSpace;

        assertEquals(expectedFreeSpace, page.getFreeSpace());
    }
}
