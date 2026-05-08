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
 *
 * 注意: PageManager构造器预分配page 0给根节点,所以初始状态:
 * - allocatedPages包含page 0 (cardinality=1)
 * - nextPageId=1
 */
@DisplayName("PageManager - 页分配器测试")
class PageManagerTest {

    private PageManager manager;

    @BeforeEach
    void setUp() {
        manager = new PageManager();
    }

    @Test
    @DisplayName("新创建的PageManager应该预分配根节点页")
    void testNewPageManager() {
        assertEquals(1, manager.getAllocatedPageCount()); // page 0 pre-allocated
        assertEquals(0, manager.getFreePageCount());
        assertEquals(1, manager.getNextPageId()); // starts at 1, not 0
        assertTrue(manager.isPageAllocated(0)); // page 0 is reserved
    }

    @Test
    @DisplayName("分配页应该递增页号")
    void testAllocatePage() {
        int page1 = manager.allocatePage();
        int page2 = manager.allocatePage();
        int page3 = manager.allocatePage();

        assertEquals(1, page1); // first allocatable page after reserved page 0
        assertEquals(2, page2);
        assertEquals(3, page3);
        assertEquals(4, manager.getAllocatedPageCount()); // pages 0,1,2,3
        assertEquals(4, manager.getNextPageId());
    }

    @Test
    @DisplayName("释放页后应该可以重用")
    void testFreeAndReusePage() {
        int page1 = manager.allocatePage();
        int page2 = manager.allocatePage();
        int page3 = manager.allocatePage();

        assertEquals(4, manager.getAllocatedPageCount()); // 0,1,2,3

        manager.freePage(page2);

        assertEquals(3, manager.getAllocatedPageCount());
        assertEquals(1, manager.getFreePageCount());
        assertFalse(manager.isPageAllocated(page2));

        int reused = manager.allocatePage();

        assertEquals(page2, reused);
        assertEquals(4, manager.getAllocatedPageCount());
        assertEquals(0, manager.getFreePageCount());
        assertTrue(manager.isPageAllocated(reused));
    }

    @Test
    @DisplayName("释放中间页后分配应该重用空闲页")
    void testReuseMiddlePage() {
        for (int i = 0; i < 5; i++) {
            manager.allocatePage();
        }

        manager.freePage(2);
        manager.freePage(3);
        manager.freePage(4);

        assertEquals(3, manager.getFreePageCount());

        int newPage1 = manager.allocatePage();
        int newPage2 = manager.allocatePage();
        int newPage3 = manager.allocatePage();

        assertTrue(newPage1 >= 2 && newPage1 <= 4);
        assertTrue(newPage2 >= 2 && newPage2 <= 4);
        assertTrue(newPage3 >= 2 && newPage3 <= 4);

        assertNotEquals(newPage1, newPage2);
        assertNotEquals(newPage2, newPage3);
        assertNotEquals(newPage1, newPage3);
    }

    @Test
    @DisplayName("释放不存在的页应该静默忽略")
    void testFreeNonExistentPage() {
        manager.allocatePage();

        assertEquals(2, manager.getAllocatedPageCount()); // page 0 + allocated page

        manager.freePage(99);

        assertEquals(2, manager.getAllocatedPageCount());
        assertEquals(0, manager.getFreePageCount());
    }

    @Test
    @DisplayName("重复释放同一页应该静默忽略")
    void testFreePageTwice() {
        int page1 = manager.allocatePage();

        manager.freePage(page1);
        assertEquals(1, manager.getAllocatedPageCount()); // only page 0 left
        assertEquals(1, manager.getFreePageCount());

        manager.freePage(page1); // free again - should be ignored

        assertEquals(1, manager.getAllocatedPageCount());
        assertEquals(1, manager.getFreePageCount());
    }

    @Test
    @DisplayName("检查页是否已分配")
    void testIsPageAllocated() {
        int page1 = manager.allocatePage();
        int page2 = manager.allocatePage();

        assertTrue(manager.isPageAllocated(0)); // reserved root page
        assertTrue(manager.isPageAllocated(page1));
        assertTrue(manager.isPageAllocated(page2));
        assertFalse(manager.isPageAllocated(99));

        manager.freePage(page1);

        assertFalse(manager.isPageAllocated(page1));
        assertTrue(manager.isPageAllocated(page2));
    }

    @Test
    @DisplayName("批量分配和释放页")
    void testBulkAllocateAndFree() {
        int[] pages = new int[100];
        for (int i = 0; i < 100; i++) {
            pages[i] = manager.allocatePage();
        }

        assertEquals(101, manager.getAllocatedPageCount()); // page 0 + 100 pages

        // free even-indexed allocated pages
        for (int i = 0; i < 100; i += 2) {
            manager.freePage(pages[i]);
        }

        assertEquals(51, manager.getAllocatedPageCount());
        assertEquals(50, manager.getFreePageCount());

        for (int i = 0; i < 50; i++) {
            manager.allocatePage();
        }

        assertEquals(101, manager.getAllocatedPageCount());
        assertEquals(0, manager.getFreePageCount());
        assertEquals(101, manager.getNextPageId());
    }

    @Test
    @DisplayName("重置页管理器")
    void testReset() {
        manager.allocatePage();
        manager.allocatePage();
        manager.freePage(1);

        assertEquals(2, manager.getAllocatedPageCount());
        assertEquals(1, manager.getFreePageCount());

        manager.reset();

        // reset clears everything including pre-allocation
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
            assertEquals(i + 1, pageId); // first page is 1 (page 0 reserved)
        }

        assertEquals(count + 1, manager.getAllocatedPageCount());
        assertEquals(count + 1, manager.getNextPageId());
    }

    @Test
    @DisplayName("空闲页优先分配策略")
    void testFreePagePriority() {
        int page1 = manager.allocatePage();
        int page2 = manager.allocatePage();

        manager.freePage(page1);

        int newPage = manager.allocatePage();

        assertEquals(page1, newPage); // reused page1
        assertEquals(3, manager.getAllocatedPageCount()); // pages 0,1,2
        assertEquals(3, manager.getNextPageId());
    }

    @Test
    @DisplayName("toString方法应该返回正确的信息")
    void testToString() {
        manager.allocatePage();
        manager.allocatePage();
        manager.freePage(1);

        String str = manager.toString();

        assertTrue(str.contains("nextPageId=3"));
        assertTrue(str.contains("allocatedPages=2")); // pages 0 and 2
        assertTrue(str.contains("freePages=1"));
    }
}
