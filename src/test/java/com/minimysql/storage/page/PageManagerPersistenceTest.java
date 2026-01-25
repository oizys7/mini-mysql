package com.minimysql.storage.page;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * PageManager持久化测试
 *
 * 测试PageManager的元数据持久化功能:
 * - 保存和加载状态
 * - 重启后状态恢复
 * - BitSet内存优化验证
 */
@DisplayName("PageManager - 元数据持久化测试")
class PageManagerPersistenceTest {

    private static final int TEST_TABLE_ID = 999;

    @BeforeEach
    void setUp() {
        // 清理测试数据
        cleanupTestData();
    }

    @AfterEach
    void tearDown() {
        // 清理测试数据
        cleanupTestData();
    }

    private void cleanupTestData() {
        try {
            Path metaPath = Path.of("data", "table_" + TEST_TABLE_ID + ".pagemeta");
            if (Files.exists(metaPath)) {
                Files.delete(metaPath);
            }
        } catch (IOException ignore) {}
    }

    @Test
    @DisplayName("首次启动:元数据文件不存在时应该初始化为空")
    void testFirstStartup() {
        PageManager manager = new PageManager();
        manager.load(TEST_TABLE_ID);

        assertEquals(0, manager.getAllocatedPageCount());
        assertEquals(0, manager.getFreePageCount());
        assertEquals(0, manager.getNextPageId());
    }

    @Test
    @DisplayName("分配页后保存应该持久化状态")
    void testSaveAfterAllocate() {
        PageManager manager = new PageManager();
        manager.load(TEST_TABLE_ID);

        // 分配3页
        int page0 = manager.allocatePage();
        int page1 = manager.allocatePage();
        int page2 = manager.allocatePage();

        assertEquals(0, page0);
        assertEquals(1, page1);
        assertEquals(2, page2);
        assertEquals(3, manager.getAllocatedPageCount());

        // 创建新的manager,模拟重启
        PageManager newManager = new PageManager();
        newManager.load(TEST_TABLE_ID);

        // 验证状态已恢复
        assertEquals(3, newManager.getAllocatedPageCount());
        assertEquals(3, newManager.getNextPageId());
        assertEquals(0, newManager.getFreePageCount());

        // 验证已分配的页
        assertTrue(newManager.isPageAllocated(0));
        assertTrue(newManager.isPageAllocated(1));
        assertTrue(newManager.isPageAllocated(2));
        assertFalse(newManager.isPageAllocated(3));
    }

    @Test
    @DisplayName("释放页后保存应该持久化空闲页")
    void testSaveAfterFree() {
        PageManager manager = new PageManager();
        manager.load(TEST_TABLE_ID);

        // 分配5页
        for (int i = 0; i < 5; i++) {
            manager.allocatePage();
        }

        // 释放页1、2、3
        manager.freePage(1);
        manager.freePage(2);
        manager.freePage(3);

        assertEquals(2, manager.getAllocatedPageCount());
        assertEquals(3, manager.getFreePageCount());

        // 模拟重启
        PageManager newManager = new PageManager();
        newManager.load(TEST_TABLE_ID);

        // 验证状态已恢复
        assertEquals(2, newManager.getAllocatedPageCount());
        assertEquals(3, newManager.getFreePageCount());
        assertEquals(5, newManager.getNextPageId());

        // 验证哪些页已分配
        assertTrue(newManager.isPageAllocated(0));
        assertFalse(newManager.isPageAllocated(1)); // 已释放
        assertFalse(newManager.isPageAllocated(2)); // 已释放
        assertFalse(newManager.isPageAllocated(3)); // 已释放
        assertTrue(newManager.isPageAllocated(4));
    }

    @Test
    @DisplayName("重启后分配应该重用空闲页")
    void testReuseFreePageAfterRestart() {
        PageManager manager = new PageManager();
        manager.load(TEST_TABLE_ID);

        // 分配3页
        manager.allocatePage(); // 0
        manager.allocatePage(); // 1
        manager.allocatePage(); // 2

        // 释放页1
        manager.freePage(1);

        // 模拟重启
        PageManager newManager = new PageManager();
        newManager.load(TEST_TABLE_ID);

        // 分配新页,应该重用页1
        int newPage = newManager.allocatePage();

        assertEquals(1, newPage);
        assertEquals(3, newManager.getAllocatedPageCount());
        assertEquals(0, newManager.getFreePageCount());
    }

    @Test
    @DisplayName("元数据文件应该包含正确的数据")
    void testMetadataFileContent() throws IOException {
        PageManager manager = new PageManager();
        manager.load(TEST_TABLE_ID);

        // 分配一些页
        manager.allocatePage(); // 0
        manager.allocatePage(); // 1
        manager.allocatePage(); // 2

        // 释放页1
        manager.freePage(1);

        // 读取元数据文件
        Path metaPath = Path.of("data", "table_" + TEST_TABLE_ID + ".pagemeta");
        assertTrue(Files.exists(metaPath));

        byte[] data = Files.readAllBytes(metaPath);
        PageMetadata metadata = PageMetadata.fromBytes(data);

        // 验证元数据内容
        assertEquals(3, metadata.getNextPageId());
        assertEquals(1, metadata.getFreePages().size());
        assertTrue(metadata.getFreePages().contains(1));
    }

    @Test
    @DisplayName("BitSet内存占用应该远小于HashSet")
    @SuppressWarnings("unused")
    void testBitSetMemoryEfficiency() {
        // 注意:不加载tableId,避免频繁写磁盘
        PageManager manager = new PageManager();

        // 分配10000页
        for (int i = 0; i < 10000; i++) {
            manager.allocatePage();
        }

        // 如果使用HashSet<Integer>,10000个Integer对象约占用360KB
        // 使用BitSet,10000个bit约占用1.25KB
        // 我们无法直接测量内存占用,但可以验证功能正确

        assertEquals(10000, manager.getAllocatedPageCount());

        // 释放一半的页
        for (int i = 0; i < 5000; i++) {
            manager.freePage(i * 2);
        }

        assertEquals(5000, manager.getAllocatedPageCount());
        assertEquals(5000, manager.getFreePageCount());

        // 验证BitSet的功能
        for (int i = 0; i < 10000; i++) {
            boolean expected = (i % 2 != 0); // 奇数页已分配
            assertEquals(expected, manager.isPageAllocated(i),
                    "Page " + i + " allocation status mismatch");
        }
    }

    @Test
    @DisplayName("大规模分配测试BitSet性能")
    void testLargeScaleAllocation() {
        // 注意:此测试不加载tableId,避免频繁写磁盘
        PageManager manager = new PageManager();

        final int pageCount = 100000; // 10万页

        // 分配10万页
        long startAlloc = System.nanoTime();
        for (int i = 0; i < pageCount; i++) {
            manager.allocatePage();
        }
        long allocTime = System.nanoTime() - startAlloc;

        assertEquals(pageCount, manager.getAllocatedPageCount());

        // 释放一半
        long startFree = System.nanoTime();
        for (int i = 0; i < pageCount / 2; i++) {
            manager.freePage(i * 2);
        }
        long freeTime = System.nanoTime() - startFree;

        assertEquals(pageCount / 2, manager.getAllocatedPageCount());

        // 断言:应该在合理时间内完成(1秒内)
        assertTrue(allocTime < 1_000_000_000, "Allocation too slow");
        assertTrue(freeTime < 1_000_000_000, "Free too slow");
    }

    @Test
    @DisplayName("重启后大规模状态恢复")
    void testLargeScaleRestore() {
        PageManager manager = new PageManager();
        manager.load(TEST_TABLE_ID);

        // 分配1000页
        for (int i = 0; i < 1000; i++) {
            manager.allocatePage();
        }

        // 释放200页
        for (int i = 0; i < 200; i++) {
            manager.freePage(i);
        }

        // 模拟重启
        PageManager newManager = new PageManager();
        newManager.load(TEST_TABLE_ID);

        // 验证状态完全恢复
        assertEquals(800, newManager.getAllocatedPageCount());
        assertEquals(200, newManager.getFreePageCount());
        assertEquals(1000, newManager.getNextPageId());

        // 验证页分配状态
        for (int i = 0; i < 1000; i++) {
            boolean expected = (i >= 200); // 前200个已释放
            assertEquals(expected, newManager.isPageAllocated(i),
                    "Page " + i + " status mismatch after restore");
        }
    }

    @Test
    @DisplayName("删除元数据文件后应该恢复为初始状态")
    void testDeleteMetadata() {
        PageManager manager = new PageManager();
        manager.load(TEST_TABLE_ID);

        // 分配一些页
        for (int i = 0; i < 10; i++) {
            manager.allocatePage();
        }

        assertEquals(10, manager.getAllocatedPageCount());

        // 删除元数据
        manager.deleteMetadata();

        // 创建新manager,应该为空
        PageManager newManager = new PageManager();
        newManager.load(TEST_TABLE_ID);

        assertEquals(0, newManager.getAllocatedPageCount());
        assertEquals(0, newManager.getFreePageCount());
    }

    @Test
    @DisplayName("多次重启应该保持状态一致性")
    void testMultipleRestarts() {
        // 第一次启动
        PageManager manager1 = new PageManager();
        manager1.load(TEST_TABLE_ID);

        for (int i = 0; i < 5; i++) {
            manager1.allocatePage();
        }
        manager1.freePage(2);

        // 第二次启动
        PageManager manager2 = new PageManager();
        manager2.load(TEST_TABLE_ID);

        assertEquals(4, manager2.getAllocatedPageCount());
        assertEquals(1, manager2.getFreePageCount());

        // 再分配2页
        manager2.allocatePage(); // 应该重用页2
        manager2.allocatePage(); // 新页5

        // 第三次启动
        PageManager manager3 = new PageManager();
        manager3.load(TEST_TABLE_ID);

        assertEquals(6, manager3.getAllocatedPageCount());
        assertEquals(0, manager3.getFreePageCount());
        assertEquals(6, manager3.getNextPageId());
    }
}
