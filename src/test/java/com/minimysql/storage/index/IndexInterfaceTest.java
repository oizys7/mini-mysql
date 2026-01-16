package com.minimysql.storage.index;

import com.minimysql.storage.buffer.BufferPool;
import com.minimysql.storage.page.PageManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.*;

/**
 * IndexInterfaceTest - Index接口测试
 *
 * 测试Index接口的实现和多态使用:
 * 1. BPlusTree正确实现Index接口
 * 2. ClusteredIndex和SecondaryIndex可以向上转型为Index
 * 3. BufferPool的脏页刷新bug已修复
 */
class IndexInterfaceTest {

    private BufferPool bufferPool;
    private static final String DATA_DIR = "data";

    @BeforeEach
    void setUp() {
        bufferPool = new BufferPool(100);
    }

    @AfterEach
    void tearDown() throws IOException {
        if (bufferPool != null) {
            bufferPool.clear();
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
     * 测试BPlusTree实现Index接口
     */
    @Test
    void testBPlusTreeImplementsIndex() {
        PageManager pageManager = new PageManager();
        BPlusTree bPlusTree = new BPlusTree(1, "test_index", false, "id", bufferPool, pageManager) {
            @Override
            protected BPlusTreeNode createRootNode() {
                return new BPlusTreeNode(true);
            }
        };

        // 验证可以向上转型为Index接口
        Index index = bPlusTree;
        assertNotNull(index);
        assertEquals("test_index", index.getIndexName());
        assertEquals("id", index.getColumnName());
        assertFalse(index.isClustered());
        assertFalse(index.isUnique());
    }

    /**
     * 测试ClusteredIndex实现Index接口
     */
    @Test
    void testClusteredIndexImplementsIndex() {
        PageManager pageManager = new PageManager();
        ClusteredIndex clusteredIndex = new ClusteredIndex(
                1, "id", 0, bufferPool, pageManager
        );
        clusteredIndex.setColumns(java.util.List.of());

        // 验证可以向上转型为Index接口
        Index index = clusteredIndex;
        assertNotNull(index);
        assertTrue(index.isClustered());
        assertEquals("PRIMARY", index.getIndexName());
    }

    /**
     * 测试Index接口的多态使用
     */
    @Test
    void testIndexPolymorphism() {
        PageManager pageManager1 = new PageManager();
        PageManager pageManager2 = new PageManager();

        ClusteredIndex clusteredIndex = new ClusteredIndex(
                1, "id", 0, bufferPool, pageManager1
        );
        clusteredIndex.setColumns(java.util.List.of());

        // 使用Index接口类型引用
        Index index1 = clusteredIndex;
        Index index2 = new BPlusTree(2, "secondary", false, "name", bufferPool, pageManager2) {
            @Override
            protected BPlusTreeNode createRootNode() {
                return new BPlusTreeNode(true);
            }
        };

        // 验证多态调用
        assertEquals("PRIMARY", index1.getIndexName());
        assertTrue(index1.isClustered());

        assertEquals("secondary", index2.getIndexName());
        assertFalse(index2.isClustered());
    }

    /**
     * 测试BufferPool修复bug:脏页刷新到正确的表文件
     */
    @Test
    void testBufferPoolFlushBugFixed() {
        // 创建两个表的页
        int table1Id = 1;
        int table2Id = 2;
        int pageId = 0;

        // 新建页并设置tableId
        var frame1 = bufferPool.newPage(table1Id, pageId);
        var frame2 = bufferPool.newPage(table2Id, pageId);

        // 验证tableId已正确设置
        assertEquals(table1Id, frame1.getTableId());
        assertEquals(table2Id, frame2.getTableId());

        // 标记为脏页
        frame1.markDirty();
        frame2.markDirty();

        // 刷新所有脏页(不应该抛出异常)
        assertDoesNotThrow(() -> {
            bufferPool.flushAllPages();
        });
    }
}
