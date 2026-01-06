package com.minimysql.storage.buffer;

import com.minimysql.storage.page.DataPage;
import com.minimysql.storage.page.Page;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * BufferPool单元测试
 *
 * 测试缓冲池的核心功能:
 * - 页的获取和创建
 * - LRU淘汰机制
 * - 脏页管理
 * - 并发访问安全
 * - 磁盘读写持久化
 */
class BufferPoolTest {

    private BufferPool bufferPool;

    private static final int TABLE_ID = 0;

    @BeforeEach
    void setUp() {
        // 创建小缓冲池(只有3页),方便测试LRU淘汰
        bufferPool = new BufferPool(3);
    }

    @AfterEach
    void tearDown() {
        // 清理缓冲池
        bufferPool.clear();

        // 删除测试数据文件
        try {
            Path dataDir = Path.of("data");
            if (Files.exists(dataDir)) {
                Files.walk(dataDir)
                        .sorted((a, b) -> b.compareTo(a)) // 反向排序,先删除文件
                        .forEach(path -> {
                            try {
                                Files.delete(path);
                            } catch (IOException e) {
                                // Ignore
                            }
                        });
            }
        } catch (IOException e) {
            // Ignore
        }
    }

    @Test
    @DisplayName("创建新的缓冲池应该为空")
    void testNewBufferPool() {
        assertEquals(0, bufferPool.getCacheSize());
        assertEquals(3, bufferPool.getPoolSize());
    }

    @Test
    @DisplayName("创建新页应该成功")
    void testNewPage() {
        PageFrame frame = bufferPool.newPage(TABLE_ID, 0);

        assertNotNull(frame);
        assertEquals(0, frame.getPinCount());
        assertFalse(frame.isDirty());
        assertEquals(0, frame.getPage().getPageId());
        assertEquals(1, bufferPool.getCacheSize());
    }

    @Test
    @DisplayName("获取已存在的页应该从缓存返回")
    void testGetPageFromCache() {
        bufferPool.newPage(TABLE_ID, 0);

        PageFrame frame1 = bufferPool.getPage(TABLE_ID, 0);
        PageFrame frame2 = bufferPool.getPage(TABLE_ID, 0);

        // 应该是同一个页帧对象
        assertSame(frame1, frame2);
        assertEquals(1, bufferPool.getCacheSize());
    }

    @Test
    @DisplayName("获取不存在的页应该从磁盘读取(返回空页)")
    void testGetNonExistentPage() {
        PageFrame frame = bufferPool.getPage(TABLE_ID, 99);

        assertNotNull(frame);
        assertEquals(99, frame.getPage().getPageId());
        assertEquals(1, bufferPool.getCacheSize());
    }

    @Test
    @DisplayName("页的pin和unpin应该配对")
    void testPinUnpin() {
        PageFrame frame = bufferPool.newPage(TABLE_ID, 0);

        frame.pin();
        assertEquals(1, frame.getPinCount());

        frame.unpin(false);
        assertEquals(0, frame.getPinCount());
    }

    @Test
    @DisplayName("多次pin需要多次unpin才能归零")
    void testMultiplePins() {
        PageFrame frame = bufferPool.newPage(TABLE_ID, 0);

        frame.pin();
        frame.pin();
        frame.pin();
        assertEquals(3, frame.getPinCount());

        frame.unpin(false);
        frame.unpin(false);
        assertEquals(1, frame.getPinCount());

        frame.unpin(false);
        assertEquals(0, frame.getPinCount());
    }

    @Test
    @DisplayName("unpin超过pin次数应该抛异常")
    void testOverUnpin() {
        PageFrame frame = bufferPool.newPage(TABLE_ID, 0);

        frame.pin();
        frame.unpin(false);

        assertThrows(IllegalStateException.class, () -> frame.unpin(false));
    }

    @Test
    @DisplayName("标记脏页应该设置dirty标记")
    void testMarkDirty() {
        PageFrame frame = bufferPool.newPage(TABLE_ID, 0);

        assertFalse(frame.isDirty());

        frame.markDirty();
        assertTrue(frame.isDirty());
    }

    @Test
    @DisplayName("unpin时标记dirty应该设置脏标记")
    void testUnpinWithDirty() {
        PageFrame frame = bufferPool.newPage(TABLE_ID, 0);
        frame.pin();

        frame.unpin(true);

        assertTrue(frame.isDirty());
    }

    @Test
    @DisplayName("pinCount为0的页可以被淘汰")
    void testEvictable() {
        PageFrame frame = bufferPool.newPage(TABLE_ID, 0);

        assertTrue(frame.isEvictable());

        frame.pin();
        assertFalse(frame.isEvictable());

        frame.unpin(false);
        assertTrue(frame.isEvictable());
    }

    @Test
    @DisplayName("LRU淘汰:缓冲池满时应该淘汰最久未使用的页")
    void testLRUEviction() {
        // 填满缓冲池(3页)
        PageFrame frame0 = bufferPool.newPage(TABLE_ID, 0);
        PageFrame frame1 = bufferPool.newPage(TABLE_ID, 1);
        PageFrame frame2 = bufferPool.newPage(TABLE_ID, 2);

        assertEquals(3, bufferPool.getCacheSize());

        // 访问frame0,更新其LRU位置
        bufferPool.getPage(TABLE_ID, 0);

        // 添加第4页,应该淘汰frame1(最久未使用)
        PageFrame frame3 = bufferPool.newPage(TABLE_ID, 3);

        assertEquals(3, bufferPool.getCacheSize());

        // frame0和frame2应该还在缓存中
        assertSame(frame0, bufferPool.getPage(TABLE_ID, 0));
        assertSame(frame2, bufferPool.getPage(TABLE_ID, 2));

        // frame1应该被淘汰了(重新加载是新的对象)
        PageFrame frame1Reloaded = bufferPool.getPage(TABLE_ID, 1);
        assertNotSame(frame1, frame1Reloaded);
    }

    @Test
    @DisplayName("LRU淘汰:被pin住的页不能被淘汰")
    void testPinnedPageNotEvicted() {
        // 填满缓冲池
        PageFrame frame0 = bufferPool.newPage(TABLE_ID, 0);
        PageFrame frame1 = bufferPool.newPage(TABLE_ID, 1);
        PageFrame frame2 = bufferPool.newPage(TABLE_ID, 2);

        // pin住frame1
        frame1.pin();

        // 多次访问frame0和frame2
        bufferPool.getPage(TABLE_ID, 0);
        bufferPool.getPage(TABLE_ID, 2);
        bufferPool.getPage(TABLE_ID, 0);
        bufferPool.getPage(TABLE_ID, 2);

        // 添加第4页,应该淘汰frame0或frame2,而不是被pin的frame1
        bufferPool.newPage(TABLE_ID, 3);

        // frame1应该还在缓存中
        assertSame(frame1, bufferPool.getPage(TABLE_ID, 1));
        assertEquals(1, frame1.getPinCount());

        frame1.unpin(false);
    }

    @Test
    @DisplayName("脏页在淘汰前应该写回磁盘")
    void testDirtyPageFlushOnEviction() {
        // 创建页并插入数据
        PageFrame frame0 = bufferPool.newPage(TABLE_ID, 0);
        DataPage page0 = (DataPage) frame0.getPage();
        byte[] rowData = "Test data for dirty page".getBytes();
        page0.insertRow(rowData);

        // 标记为脏页
        frame0.markDirty();

        // 填满缓冲池
        bufferPool.newPage(TABLE_ID, 1);
        bufferPool.newPage(TABLE_ID, 2);

        // 添加第4页,触发淘汰frame0
        bufferPool.newPage(TABLE_ID, 3);

        // 清空缓冲池
        bufferPool.clear();

        // 重新加载页,验证数据已持久化
        PageFrame reloaded = bufferPool.getPage(TABLE_ID, 0);
        DataPage reloadedPage = (DataPage) reloaded.getPage();
        byte[] reloadedData = reloadedPage.getRow(0);

        assertArrayEquals(rowData, reloadedData);
    }

    @Test
    @DisplayName("刷新页应该写回磁盘")
    void testFlushPage() {
        PageFrame frame = bufferPool.newPage(TABLE_ID, 0);
        DataPage page = (DataPage) frame.getPage();
        byte[] rowData = "Flush test data".getBytes();
        page.insertRow(rowData);
        frame.markDirty();

        // 刷新页
        bufferPool.flushPage(TABLE_ID, 0);

        // 验证脏标记已清除
        assertFalse(frame.isDirty());

        // 清空缓存
        bufferPool.clear();

        // 重新加载,验证数据持久化
        PageFrame reloaded = bufferPool.getPage(TABLE_ID, 0);
        DataPage reloadedPage = (DataPage) reloaded.getPage();
        byte[] reloadedData = reloadedPage.getRow(0);

        assertArrayEquals(rowData, reloadedData);
    }

    @Test
    @DisplayName("刷新所有页应该写回所有脏页")
    void testFlushAllPages() {
        // 创建多个脏页
        PageFrame frame0 = bufferPool.newPage(TABLE_ID, 0);
        ((DataPage) frame0.getPage()).insertRow("Data 0".getBytes());
        frame0.markDirty();

        PageFrame frame1 = bufferPool.newPage(TABLE_ID, 1);
        ((DataPage) frame1.getPage()).insertRow("Data 1".getBytes());
        frame1.markDirty();

        // 刷新所有页
        bufferPool.flushAllPages();

        // 验证脏标记已清除
        assertFalse(frame0.isDirty());
        assertFalse(frame1.isDirty());
    }

    @Test
    @DisplayName("清空缓冲池应该写回所有脏页并清空缓存")
    void testClear() {
        // 创建脏页
        PageFrame frame = bufferPool.newPage(TABLE_ID, 0);
        ((DataPage) frame.getPage()).insertRow("Data".getBytes());
        frame.markDirty();

        // 清空缓冲池
        bufferPool.clear();

        // 验证缓存已清空
        assertEquals(0, bufferPool.getCacheSize());
    }

    @Test
    @DisplayName("并发访问应该安全")
    void testConcurrentAccess() throws InterruptedException {
        int threadCount = 10;
        int operationsPerThread = 100;

        Thread[] threads = new Thread[threadCount];

        for (int i = 0; i < threadCount; i++) {
            final int threadId = i;
            threads[i] = new Thread(() -> {
                for (int j = 0; j < operationsPerThread; j++) {
                    int pageId = (threadId + j) % 5; // 使用5个不同的页
                    PageFrame frame = bufferPool.getPage(TABLE_ID, pageId);
                    frame.pin();
                    try {
                        // 模拟使用页
                        Thread.sleep(1);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        frame.unpin(j % 2 == 0);
                    }
                }
            });
        }

        for (Thread thread : threads) {
            thread.start();
        }

        for (Thread thread : threads) {
            thread.join();
        }

        // 验证缓冲池状态正常
        assertTrue(bufferPool.getCacheSize() <= 5);
    }
}
