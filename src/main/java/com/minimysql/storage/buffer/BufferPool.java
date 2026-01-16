package com.minimysql.storage.buffer;

import com.minimysql.storage.page.DataPage;
import com.minimysql.storage.page.Page;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

/**
 * BufferPool - 缓冲池管理器
 * todo 优化阶段，尝试优化锁的粒度/是否可以不使用锁
 *
 * 缓冲池是数据库性能的核心,用于缓存磁盘上的页,减少磁盘I/O。
 *
 * 核心功能:
 * 1. 页缓存:在内存中缓存固定数量的页
 * 2. LRU淘汰:当缓存满时,淘汰最久未使用的页
 * 3. 脏页管理:被修改的页在淘汰前写回磁盘
 * 4. 引用计数:正在使用的页不能被淘汰
 *
 * 使用模式:
 * <pre>
 * // 获取页(自动从磁盘读取)
 * PageFrame frame = bufferPool.getPage(pageId);
 * frame.pin();  // 增加引用计数
 *
 * // 修改页
 * DataPage page = (DataPage) frame.getPage();
 * page.insertRow(rowData);
 *
 * // 释放页
 * frame.unpin(true);  // 标记为脏页并减少引用计数
 * </pre>
 *
 * 设计哲学:
 * - 使用LinkedHashMap实现LRU,访问时将页移到链表尾部
 * - pinCount > 0的页不能被淘汰
 * - 脏页淘汰前必须写回磁盘
 * - 文件操作简单直接:每页对应文件的某个偏移量
 *
 * "Good taste": 没有复杂的预取、多缓冲池、自适应淘汰等,只有最基本的LRU
 *
 * "实用主义": 文件直接存储在data/目录,每个表一个.db文件
 */
public class BufferPool {

    /** 默认缓冲池大小:100页 */
    public static final int DEFAULT_POOL_SIZE = 100;

    /** 数据目录 */
    private static final String DATA_DIR = "data";

    /** 缓冲池大小(页数) */
    private final int poolSize;

    /** LRU缓存:页号 → 页帧 */
    private final LinkedHashMap<Integer, PageFrame> pageCache;

    /** 缓冲池锁(保证并发安全) */
    private final ReentrantLock lock;

    /** 数据目录路径 */
    private final Path dataDirPath;

    /**
     * 创建默认大小(100页)的缓冲池
     */
    public BufferPool() {
        this(DEFAULT_POOL_SIZE);
    }

    /**
     * 创建指定大小的缓冲池
     *
     * @param poolSize 缓冲池大小(页数)
     */
    public BufferPool(int poolSize) {
        this.poolSize = poolSize;
        this.lock = new ReentrantLock();

        // 使用LinkedHashMap实现LRU:按访问顺序排序
        // 注意:不使用removeEldestEntry自动淘汰,而是手动控制
        this.pageCache = new LinkedHashMap<>(16, 0.75f, true);

        // 确保数据目录存在
        this.dataDirPath = Path.of(DATA_DIR);
        try {
            if (!Files.exists(dataDirPath)) {
                Files.createDirectories(dataDirPath);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to create data directory: " + DATA_DIR, e);
        }
    }

    /**
     * 获取页
     *
     * 如果页在缓存中,直接返回。
     * 如果页不在缓存中,从磁盘读取并加入缓存。
     *
     * @param tableId 表ID
     * @param pageId 页号
     * @return 页帧
     */
    public PageFrame getPage(int tableId, int pageId) {
        lock.lock();

        try {
            // 构造缓存键:tableId * 1M + pageId (简单避免冲突)
            int cacheKey = tableId * 1_000_000 + pageId;

            // 尝试从缓存获取
            PageFrame frame = pageCache.get(cacheKey);

            if (frame == null) {
                // 缓存未命中,检查是否需要淘汰
                if (pageCache.size() >= poolSize) {
                    evictOnePage();
                }

                // 从磁盘读取
                frame = loadPageFromDisk(tableId, pageId);
                pageCache.put(cacheKey, frame);
            }

            return frame;
        } finally {
            lock.unlock();
        }
    }

    /**
     * 创建新页
     *
     * 分配一个新的页,用于插入新数据。
     *
     * @param tableId 表ID
     * @param pageId 页号
     * @return 页帧
     */
    public PageFrame newPage(int tableId, int pageId) {
        lock.lock();

        try {
            int cacheKey = tableId * 1_000_000 + pageId;

            // 如果页已存在,抛出异常
            if (pageCache.containsKey(cacheKey)) {
                throw new IllegalArgumentException("Page already exists: tableId=" + tableId + ", pageId=" + pageId);
            }

            // 如果缓存已满,淘汰一页
            if (pageCache.size() >= poolSize) {
                evictOnePage();
            }

            // 创建新页
            DataPage page = new DataPage();
            page.setPageId(pageId);
            PageFrame frame = new PageFrame(page);
            frame.setTableId(tableId);  // 设置tableId

            pageCache.put(cacheKey, frame);

            return frame;
        } finally {
            lock.unlock();
        }
    }

    /**
     * 刷新页到磁盘
     *
     * 将页写入磁盘文件,无论是否为脏页。
     *
     * @param tableId 表ID
     * @param pageId 页号
     */
    public void flushPage(int tableId, int pageId) {
        lock.lock();

        try {
            int cacheKey = tableId * 1_000_000 + pageId;
            PageFrame frame = pageCache.get(cacheKey);

            if (frame != null) {
                writePageToDisk(tableId, frame);
                frame.clearDirty();
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * 刷新所有脏页到磁盘
     */
    public void flushAllPages() {
        lock.lock();

        try {
            for (PageFrame frame : pageCache.values()) {
                if (frame.isDirty()) {
                    // 使用PageFrame存储的tableId,而不是错误推断
                    int tableId = frame.getTableId();
                    writePageToDisk(tableId, frame);
                    frame.clearDirty();
                }
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * 获取缓冲池大小
     */
    public int getPoolSize() {
        return poolSize;
    }

    /**
     * 获取缓存的页数
     */
    public int getCacheSize() {
        lock.lock();
        try {
            return pageCache.size();
        } finally {
            lock.unlock();
        }
    }

    /**
     * 清空缓冲池
     *
     * 将所有脏页写回磁盘,然后清空缓存。
     */
    public void clear() {
        lock.lock();

        try {
            flushAllPages();
            pageCache.clear();
        } finally {
            lock.unlock();
        }
    }

    /**
     * 从磁盘加载页
     *
     * @param tableId 表ID
     * @param pageId 页号
     * @return 页帧
     */
    private PageFrame loadPageFromDisk(int tableId, int pageId) {
        Path filePath = getTableFilePath(tableId);

        try {
            if (!Files.exists(filePath)) {
                // 文件不存在,返回空页
                DataPage page = new DataPage();
                page.setPageId(pageId);
                PageFrame frame = new PageFrame(page);
                frame.setTableId(tableId);  // 设置tableId
                return frame;
            }

            // 读取文件
            byte[] fileData = Files.readAllBytes(filePath);

            // 计算页在文件中的偏移量
            int offset = pageId * Page.PAGE_SIZE;

            if (offset + Page.PAGE_SIZE > fileData.length) {
                // 文件不够大,返回空页
                DataPage page = new DataPage();
                page.setPageId(pageId);
                PageFrame frame = new PageFrame(page);
                frame.setTableId(tableId);  // 设置tableId
                return frame;
            }

            // 提取页数据
            byte[] pageData = new byte[Page.PAGE_SIZE];
            System.arraycopy(fileData, offset, pageData, 0, Page.PAGE_SIZE);

            // 反序列化页
            DataPage page = new DataPage();
            page.fromBytes(pageData);

            PageFrame frame = new PageFrame(page);
            frame.setTableId(tableId);  // 设置tableId
            return frame;

        } catch (IOException e) {
            throw new RuntimeException("Failed to load page from disk: tableId=" + tableId + ", pageId=" + pageId, e);
        }
    }

    /**
     * 将页写入磁盘
     *
     * @param tableId 表ID
     * @param frame 页帧
     */
    private void writePageToDisk(int tableId, PageFrame frame) {
        Path filePath = getTableFilePath(tableId);
        Page page = frame.getPage();
        int pageId = page.getPageId();

        try {
            byte[] pageData = page.toBytes();

            // 读取现有文件(如果存在)
            byte[] fileData;
            if (Files.exists(filePath)) {
                fileData = Files.readAllBytes(filePath);
            } else {
                fileData = new byte[0];
            }

            // 确保文件足够大
            int requiredSize = (pageId + 1) * Page.PAGE_SIZE;
            if (fileData.length < requiredSize) {
                byte[] newFileData = new byte[requiredSize];
                System.arraycopy(fileData, 0, newFileData, 0, fileData.length);
                fileData = newFileData;
            }

            // 写入页数据
            int offset = pageId * Page.PAGE_SIZE;
            System.arraycopy(pageData, 0, fileData, offset, Page.PAGE_SIZE);

            // 写回文件
            Files.write(filePath, fileData, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);

        } catch (IOException e) {
            throw new RuntimeException("Failed to write page to disk: tableId=" + tableId + ", pageId=" + pageId, e);
        }
    }

    /**
     * 淘汰一个页
     * todo 优化阶段，尝试优化算法
     *
     * 从缓存中找到最久未使用且未被pin的页进行淘汰。
     * 如果所有页都被pin,抛出异常(不应该发生,但防御性编程)。
     *
     * 算法:遍历LinkedHashMap的entrySet,找到第一个可淘汰的页。
     */
    private void evictOnePage() {
        for (Map.Entry<Integer, PageFrame> entry : pageCache.entrySet()) {
            PageFrame frame = entry.getValue();

            if (frame.isEvictable()) {
                // 写回脏页
                if (frame.isDirty()) {
                    // 使用PageFrame存储的tableId,而不是错误推断
                    int tableId = frame.getTableId();
                    writePageToDisk(tableId, frame);
                }

                // 从缓存中移除
                pageCache.remove(entry.getKey());
                return;
            }
        }

        // 如果所有页都被pin,抛出异常
        throw new IllegalStateException("Cannot evict page: all pages are pinned");
    }

    /**
     * 获取表文件路径
     *
     * @param tableId 表ID
     * @return 文件路径
     */
    private Path getTableFilePath(int tableId) {
        return dataDirPath.resolve("table_" + tableId + ".db");
    }
}
