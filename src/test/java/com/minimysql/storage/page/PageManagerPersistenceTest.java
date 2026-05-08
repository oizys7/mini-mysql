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
 * 注意: PageManager构造器预分配page 0给根节点
 * - 初始: allocatedPages={0}, nextPageId=1
 * - allocatePage()首次返回1
 */
@DisplayName("PageManager - 元数据持久化测试")
class PageManagerPersistenceTest {

    private static final int TEST_TABLE_ID = 999;

    @BeforeEach
    void setUp() {
        cleanupTestData();
    }

    @AfterEach
    void tearDown() {
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
    @DisplayName("首次启动:元数据文件不存在时应该有预分配的根节点页")
    void testFirstStartup() {
        PageManager manager = new PageManager();
        manager.load(TEST_TABLE_ID);

        assertEquals(1, manager.getAllocatedPageCount()); // page 0 pre-allocated
        assertEquals(0, manager.getFreePageCount());
        assertEquals(1, manager.getNextPageId());
    }

    @Test
    @DisplayName("分配页后保存应该持久化状态")
    void testSaveAfterAllocate() {
        PageManager manager = new PageManager();
        manager.load(TEST_TABLE_ID);

        int page1 = manager.allocatePage();
        int page2 = manager.allocatePage();
        int page3 = manager.allocatePage();

        assertEquals(1, page1); // first allocatable page (0 is reserved)
        assertEquals(2, page2);
        assertEquals(3, page3);
        assertEquals(4, manager.getAllocatedPageCount()); // pages 0,1,2,3

        PageManager newManager = new PageManager();
        newManager.load(TEST_TABLE_ID);

        assertEquals(4, newManager.getAllocatedPageCount());
        assertEquals(4, newManager.getNextPageId());
        assertEquals(0, newManager.getFreePageCount());

        assertTrue(newManager.isPageAllocated(0));
        assertTrue(newManager.isPageAllocated(1));
        assertTrue(newManager.isPageAllocated(2));
        assertTrue(newManager.isPageAllocated(3));
        assertFalse(newManager.isPageAllocated(4));
    }

    @Test
    @DisplayName("释放页后保存应该持久化空闲页")
    void testSaveAfterFree() {
        PageManager manager = new PageManager();
        manager.load(TEST_TABLE_ID);

        for (int i = 0; i < 5; i++) {
            manager.allocatePage(); // allocates pages 1-5
        }

        manager.freePage(1);
        manager.freePage(2);
        manager.freePage(3);

        assertEquals(3, manager.getAllocatedPageCount()); // pages 0,4,5
        assertEquals(3, manager.getFreePageCount());       // pages 1,2,3

        PageManager newManager = new PageManager();
        newManager.load(TEST_TABLE_ID);

        assertEquals(3, newManager.getAllocatedPageCount());
        assertEquals(3, newManager.getFreePageCount());
        assertEquals(6, newManager.getNextPageId());

        assertTrue(newManager.isPageAllocated(0));  // pre-allocated root
        assertFalse(newManager.isPageAllocated(1));  // freed
        assertFalse(newManager.isPageAllocated(2));  // freed
        assertFalse(newManager.isPageAllocated(3));  // freed
        assertTrue(newManager.isPageAllocated(4));
        assertTrue(newManager.isPageAllocated(5));
    }

    @Test
    @DisplayName("重启后分配应该重用空闲页")
    void testReuseFreePageAfterRestart() {
        PageManager manager = new PageManager();
        manager.load(TEST_TABLE_ID);

        manager.allocatePage(); // 1
        manager.allocatePage(); // 2
        manager.allocatePage(); // 3

        manager.freePage(2);

        PageManager newManager = new PageManager();
        newManager.load(TEST_TABLE_ID);

        int newPage = newManager.allocatePage();

        assertEquals(2, newPage); // reused
        assertEquals(4, newManager.getAllocatedPageCount()); // pages 0,1,2,3
        assertEquals(0, newManager.getFreePageCount());
    }

    @Test
    @DisplayName("元数据文件应该包含正确的数据")
    void testMetadataFileContent() throws IOException {
        PageManager manager = new PageManager();
        manager.load(TEST_TABLE_ID);

        manager.allocatePage(); // 1
        manager.allocatePage(); // 2
        manager.allocatePage(); // 3

        manager.freePage(2);

        Path metaPath = Path.of("data", "table_" + TEST_TABLE_ID + ".pagemeta");
        assertTrue(Files.exists(metaPath));

        byte[] data = Files.readAllBytes(metaPath);
        PageMetadata metadata = PageMetadata.fromBytes(data);

        assertEquals(4, metadata.getNextPageId());
        assertEquals(1, metadata.getFreePages().size());
        assertTrue(metadata.getFreePages().contains(2));
    }

    @Test
    @DisplayName("BitSet内存占用应该远小于HashSet")
    @SuppressWarnings("unused")
    void testBitSetMemoryEfficiency() {
        PageManager manager = new PageManager();

        for (int i = 0; i < 10000; i++) {
            manager.allocatePage(); // pages 1-10000
        }

        assertEquals(10001, manager.getAllocatedPageCount()); // page 0 + 10000

        // free even-indexed pages (skip page 0 - root)
        for (int i = 2; i <= 10000; i += 2) {
            manager.freePage(i);
        }

        assertEquals(5001, manager.getAllocatedPageCount()); // page 0 + 5000 odd pages
        assertEquals(5000, manager.getFreePageCount());

        for (int i = 1; i <= 10000; i++) {
            boolean expected = (i % 2 != 0); // odd pages allocated
            assertEquals(expected, manager.isPageAllocated(i),
                    "Page " + i + " allocation status mismatch");
        }
    }

    @Test
    @DisplayName("大规模分配测试BitSet性能")
    void testLargeScaleAllocation() {
        PageManager manager = new PageManager();

        final int pageCount = 100000;

        long startAlloc = System.nanoTime();
        for (int i = 0; i < pageCount; i++) {
            manager.allocatePage();
        }
        long allocTime = System.nanoTime() - startAlloc;

        assertEquals(pageCount + 1, manager.getAllocatedPageCount()); // +1 for page 0

        long startFree = System.nanoTime();
        for (int i = 0; i < pageCount / 2; i++) {
            manager.freePage(i * 2 + 2); // free even pages starting from 2
        }
        long freeTime = System.nanoTime() - startAlloc;

        assertEquals(pageCount / 2 + 1, manager.getAllocatedPageCount()); // page 0 + odd pages

        assertTrue(allocTime < 1_000_000_000, "Allocation too slow");
        assertTrue(freeTime < 1_000_000_000, "Free too slow");
    }

    @Test
    @DisplayName("重启后大规模状态恢复")
    void testLargeScaleRestore() {
        PageManager manager = new PageManager();
        manager.load(TEST_TABLE_ID);

        for (int i = 0; i < 1000; i++) {
            manager.allocatePage(); // pages 1-1000
        }

        for (int i = 1; i <= 200; i++) {
            manager.freePage(i); // free pages 1-200 (not page 0)
        }

        PageManager newManager = new PageManager();
        newManager.load(TEST_TABLE_ID);

        assertEquals(801, newManager.getAllocatedPageCount()); // page 0 + pages 201-1000 = 1 + 800
        assertEquals(200, newManager.getFreePageCount());       // pages 1-200
        assertEquals(1001, newManager.getNextPageId());

        for (int i = 0; i <= 1000; i++) {
            boolean expected;
            if (i == 0) expected = true;             // root page always allocated
            else if (i >= 1 && i <= 200) expected = false; // freed
            else expected = true;                    // allocated
            assertEquals(expected, newManager.isPageAllocated(i),
                    "Page " + i + " status mismatch after restore");
        }
    }

    @Test
    @DisplayName("删除元数据文件后应该恢复为初始状态")
    void testDeleteMetadata() {
        PageManager manager = new PageManager();
        manager.load(TEST_TABLE_ID);

        for (int i = 0; i < 10; i++) {
            manager.allocatePage();
        }

        assertEquals(11, manager.getAllocatedPageCount()); // page 0 + 10

        manager.deleteMetadata();

        PageManager newManager = new PageManager();
        newManager.load(TEST_TABLE_ID);

        // deleteMetadata only works for tableId >= 0 (TEST_TABLE_ID=999)
        // After deletion, file is gone → load keeps constructor state
        assertEquals(1, newManager.getAllocatedPageCount()); // page 0 pre-allocated
        assertEquals(0, newManager.getFreePageCount());
    }

    @Test
    @DisplayName("多次重启应该保持状态一致性")
    void testMultipleRestarts() {
        PageManager manager1 = new PageManager();
        manager1.load(TEST_TABLE_ID);

        for (int i = 0; i < 5; i++) {
            manager1.allocatePage(); // pages 1-5
        }
        manager1.freePage(3);

        PageManager manager2 = new PageManager();
        manager2.load(TEST_TABLE_ID);

        assertEquals(5, manager2.getAllocatedPageCount()); // pages 0,1,2,4,5
        assertEquals(1, manager2.getFreePageCount());       // page 3

        manager2.allocatePage(); // reuse page 3
        manager2.allocatePage(); // new page 6

        PageManager manager3 = new PageManager();
        manager3.load(TEST_TABLE_ID);

        assertEquals(7, manager3.getAllocatedPageCount()); // pages 0,1,2,3,4,5,6
        assertEquals(0, manager3.getFreePageCount());
        assertEquals(7, manager3.getNextPageId());
    }
}
