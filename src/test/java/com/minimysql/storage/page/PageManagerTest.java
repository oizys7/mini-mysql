package com.minimysql.storage.page;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * PageManager单元测试
 *
 * 测试页分配器的核心功能:
 * - 页的分配
 * - 页的释放
 * - 空闲页重用
 * - 边界情况处理
 */
class PageManagerTest {

    private PageManager manager;

    @BeforeEach
    void setUp() {
        manager = new PageManager();
    }

    @Test
    @DisplayName("新创建的PageManager应该是空的")
    void testNewPageManager() {
        assertEquals(0, manager.getAllocatedPageCount());
        assertEquals(0, manager.getFreePageCount());
        assertEquals(0, manager.getNextPageId());
    }

    @Test
    @DisplayName("分配页应该递增页号")
    void testAllocatePage() {
        int page0 = manager.allocatePage();
        int page1 = manager.allocatePage();
        int page2 = manager.allocatePage();

        assertEquals(0, page0);
        assertEquals(1, page1);
        assertEquals(2, page2);
        assertEquals(3, manager.getAllocatedPageCount());
        assertEquals(3, manager.getNextPageId());
    }

    @Test
    @DisplayName("释放页后应该可以重用")
    void testFreeAndReusePage() {
        // 分配3页
        int page0 = manager.allocatePage();
        int page1 = manager.allocatePage();
        int page2 = manager.allocatePage();

        assertEquals(3, manager.getAllocatedPageCount());

        // 释放第1页
        manager.freePage(page1);

        assertEquals(2, manager.getAllocatedPageCount());
        assertEquals(1, manager.getFreePageCount());
        assertFalse(manager.isPageAllocated(page1));

        // 分配新页,应该重用page1
        int page3 = manager.allocatePage();

        assertEquals(page1, page3); // 重用了页号1
        assertEquals(3, manager.getAllocatedPageCount());
        assertEquals(0, manager.getFreePageCount());
        assertTrue(manager.isPageAllocated(page3));
    }

    @Test
    @DisplayName("释放中间页后分配应该重用空闲页")
    void testReuseMiddlePage() {
        // 分配5页
        for (int i = 0; i < 5; i++) {
            manager.allocatePage();
        }

        // 释放页1、2、3
        manager.freePage(1);
        manager.freePage(2);
        manager.freePage(3);

        assertEquals(3, manager.getFreePageCount());

        // 分配新页,应该按某个顺序重用空闲页
        int newPage1 = manager.allocatePage();
        int newPage2 = manager.allocatePage();
        int newPage3 = manager.allocatePage();

        // 验证重用了空闲页(具体顺序取决于HashSet实现)
        assertTrue(newPage1 >= 1 && newPage1 <= 3);
        assertTrue(newPage2 >= 1 && newPage2 <= 3);
        assertTrue(newPage3 >= 1 && newPage3 <= 3);

        // 三个新页号应该不同
        assertNotEquals(newPage1, newPage2);
        assertNotEquals(newPage2, newPage3);
        assertNotEquals(newPage1, newPage3);
    }

    @Test
    @DisplayName("释放不存在的页应该静默忽略")
    void testFreeNonExistentPage() {
        manager.allocatePage();

        assertEquals(1, manager.getAllocatedPageCount());

        // 释放不存在的页
        manager.freePage(99);

        // 不应该抛异常,状态不变
        assertEquals(1, manager.getAllocatedPageCount());
        assertEquals(0, manager.getFreePageCount());
    }

    @Test
    @DisplayName("重复释放同一页应该静默忽略")
    void testFreePageTwice() {
        int page0 = manager.allocatePage();

        manager.freePage(page0);
        assertEquals(0, manager.getAllocatedPageCount());
        assertEquals(1, manager.getFreePageCount());

        // 再次释放
        manager.freePage(page0);

        // 不应该抛异常,状态不变
        assertEquals(0, manager.getAllocatedPageCount());
        assertEquals(1, manager.getFreePageCount());
    }

    @Test
    @DisplayName("检查页是否已分配")
    void testIsPageAllocated() {
        int page0 = manager.allocatePage();
        int page1 = manager.allocatePage();

        assertTrue(manager.isPageAllocated(page0));
        assertTrue(manager.isPageAllocated(page1));
        assertFalse(manager.isPageAllocated(99));

        manager.freePage(page0);

        assertFalse(manager.isPageAllocated(page0));
        assertTrue(manager.isPageAllocated(page1));
    }

    @Test
    @DisplayName("批量分配和释放页")
    void testBulkAllocateAndFree() {
        // 分配100页
        int[] pages = new int[100];
        for (int i = 0; i < 100; i++) {
            pages[i] = manager.allocatePage();
        }

        assertEquals(100, manager.getAllocatedPageCount());

        // 释放偶数页
        for (int i = 0; i < 100; i += 2) {
            manager.freePage(pages[i]);
        }

        assertEquals(50, manager.getAllocatedPageCount());
        assertEquals(50, manager.getFreePageCount());

        // 分配50页,应该重用空闲页
        for (int i = 0; i < 50; i++) {
            manager.allocatePage();
        }

        assertEquals(100, manager.getAllocatedPageCount());
        assertEquals(0, manager.getFreePageCount());
        assertEquals(100, manager.getNextPageId()); // 没有分配新页号
    }

    @Test
    @DisplayName("重置页管理器")
    void testReset() {
        // 分配一些页
        manager.allocatePage();
        manager.allocatePage();
        manager.freePage(0);

        assertEquals(1, manager.getAllocatedPageCount());
        assertEquals(1, manager.getFreePageCount());

        // 重置
        manager.reset();

        assertEquals(0, manager.getAllocatedPageCount());
        assertEquals(0, manager.getFreePageCount());
        assertEquals(0, manager.getNextPageId());
    }

    @Test
    @DisplayName("分配大量页应该正确递增")
    void testAllocateManyPages() {
        final int count = 10000;

        for (int i = 0; i < count; i++) {
            int pageId = manager.allocatePage();
            assertEquals(i, pageId);
        }

        assertEquals(count, manager.getAllocatedPageCount());
        assertEquals(count, manager.getNextPageId());
    }

    @Test
    @DisplayName("空闲页优先分配策略")
    void testFreePagePriority() {
        // 分配2页
        int page0 = manager.allocatePage(); // page 0
        int page1 = manager.allocatePage(); // page 1

        // 释放page0
        manager.freePage(page0);

        // 下次分配应该重用page0,而不是分配page2
        int newPage = manager.allocatePage();

        assertEquals(0, newPage);
        assertEquals(2, manager.getAllocatedPageCount());
        assertEquals(2, manager.getNextPageId()); // 没有递增
    }

    @Test
    @DisplayName("toString方法应该返回正确的信息")
    void testToString() {
        manager.allocatePage();
        manager.allocatePage();
        manager.freePage(0);

        String str = manager.toString();

        assertTrue(str.contains("nextPageId=2"));
        assertTrue(str.contains("allocatedPages=1"));
        assertTrue(str.contains("freePages=1"));
    }
}
