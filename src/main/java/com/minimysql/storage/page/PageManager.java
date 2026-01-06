package com.minimysql.storage.page;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;
import java.util.BitSet;

/**
 * PageManager - 页分配管理器
 *
 * PageManager负责管理表中页的分配和释放,类似内存分配器管理物理内存。
 *
 * 核心功能:
 * 1. 页分配:分配新的页号,优先重用已释放的页
 * 2. 页释放:释放不再使用的页号,供后续重用
 * 3. 页追踪:记录哪些页号正在使用
 * 4. 状态持久化:重启后恢复页分配状态
 *
 * 设计原则:
 * - 页号从0开始递增
 * - 释放的页号加入空闲集合,供后续重用
 * - 分配时优先使用空闲页,否则分配新页号
 * - 简单直接,没有复杂的段管理、区管理等
 * - 内存优化:使用BitSet而非HashSet,节省99%+内存
 *
 * 使用模式:
 * <pre>
 * PageManager manager = new PageManager();
 * manager.load(tableId);  // 启动时加载状态
 *
 * int pageId = manager.allocatePage();  // 分配页号(自动保存)
 * bufferPool.newPage(tableId, pageId);  // 在缓冲池中创建页
 *
 * manager.freePage(pageId);  // 释放页号(自动保存)
 * </pre>
 *
 * 设计哲学:
 * - "Good taste": 没有特殊情况,分配和释放逻辑统一
 * - 实用主义: 释放页时立即标记为空闲,不延迟回收
 * - 内存优化: BitSet比HashSet节省99%+内存(0.125字节 vs 36字节/页)
 * - 自动持久化: 分配/释放时自动保存到.pagemeta文件
 */
public class PageManager {

    /** 数据目录 */
    private static final String DATA_DIR = "data";

    /** 元数据文件扩展名 */
    private static final String META_FILE_EXT = ".pagemeta";

    /** 下一个将要分配的新页号 */
    private int nextPageId;

    /** 空闲页集合(已释放的页号) */
    private final Set<Integer> freePages;

    /**
     * 已分配页位图
     *
     * 使用BitSet而非HashSet<Integer>以节省内存:
     * - HashSet: ~36字节/页(对象开销+指针)
     * - BitSet: ~0.125字节/页(1 bit)
     * - 节省: 99.7%
     *
     * 示例:100万页的表
     * - HashSet: 36MB
     * - BitSet: 125KB
     */
    private final BitSet allocatedPages;

    /** 表ID(用于持久化) */
    private int tableId;

    /**
     * 创建新的页管理器
     */
    public PageManager() {
        this.nextPageId = 0;
        this.freePages = new HashSet<>();
        this.allocatedPages = new BitSet();
        this.tableId = -1;
    }

    /**
     * 从元数据文件加载状态
     *
     * 启动时调用,恢复页分配状态。
     * 如果元数据文件不存在,视为首次启动,不抛异常。
     *
     * @param tableId 表ID
     */
    public void load(int tableId) {
        this.tableId = tableId;

        Path metaPath = getMetadataFilePath(tableId);

        if (!Files.exists(metaPath)) {
            // 元数据文件不存在,视为首次启动
            return;
        }

        try {
            byte[] data = Files.readAllBytes(metaPath);
            PageMetadata metadata = PageMetadata.fromBytes(data);

            // 恢复状态
            this.nextPageId = metadata.getNextPageId();
            this.freePages.clear();
            this.freePages.addAll(metadata.getFreePages());

            // 重建allocatedPages BitSet
            this.allocatedPages.clear();
            for (int i = 0; i < nextPageId; i++) {
                if (!freePages.contains(i)) {
                    allocatedPages.set(i);
                }
            }

        } catch (IOException e) {
            throw new RuntimeException("Failed to load page metadata: tableId=" + tableId, e);
        }
    }

    /**
     * 保存状态到元数据文件
     *
     * 每次分配/释放页时自动调用,无需手动调用。
     *
     * @param tableId 表ID
     */
    public void save(int tableId) {
        this.tableId = tableId;

        PageMetadata metadata = new PageMetadata(nextPageId, freePages);
        byte[] data = metadata.toBytes();

        Path metaPath = getMetadataFilePath(tableId);

        try {
            // 确保数据目录存在
            Path dataDir = metaPath.getParent();
            if (dataDir != null && !Files.exists(dataDir)) {
                Files.createDirectories(dataDir);
            }

            // 原子写入:先写临时文件,再重命名
            Path tempPath = Path.of(metaPath + ".tmp");
            Files.write(tempPath, data);
            Files.move(tempPath, metaPath, java.nio.file.StandardCopyOption.ATOMIC_MOVE, java.nio.file.StandardCopyOption.REPLACE_EXISTING);

        } catch (IOException e) {
            throw new RuntimeException("Failed to save page metadata: tableId=" + tableId, e);
        }
    }

    /**
     * 分配一个新页号
     *
     * 分配策略:
     * 1. 如果有空闲页(之前释放的),优先重用
     * 2. 否则分配下一个新页号
     *
     * 分配后自动保存到元数据文件。
     *
     * @return 分配的页号
     */
    public int allocatePage() {
        int pageId;

        // 优先重用空闲页
        if (!freePages.isEmpty()) {
            pageId = freePages.iterator().next();
            freePages.remove(pageId);
        } else {
            // 分配新页号
            pageId = nextPageId++;
        }

        // 记录为已分配
        allocatedPages.set(pageId);

        // 自动保存
        if (tableId >= 0) {
            save(tableId);
        }

        return pageId;
    }

    /**
     * 释放页号
     *
     * 将页号标记为空闲,可以被后续分配重用。
     * 如果页号不存在或已被释放,静默忽略(实用主义)。
     *
     * 释放后自动保存到元数据文件。
     *
     * @param pageId 要释放的页号
     */
    public void freePage(int pageId) {
        // 检查页是否已分配
        if (!allocatedPages.get(pageId)) {
            // 页号不存在或已被释放,静默忽略
            return;
        }

        // 从已分配集合移除
        allocatedPages.clear(pageId);

        // 加入空闲集合
        freePages.add(pageId);

        // 自动保存
        if (tableId >= 0) {
            save(tableId);
        }
    }

    /**
     * 检查页号是否已分配
     *
     * @param pageId 页号
     * @return 如果页已分配返回true,否则返回false
     */
    public boolean isPageAllocated(int pageId) {
        return allocatedPages.get(pageId);
    }

    /**
     * 获取已分配页的数量
     *
     * @return 已分配页数
     */
    public int getAllocatedPageCount() {
        return allocatedPages.cardinality();
    }

    /**
     * 获取空闲页的数量
     *
     * @return 空闲页数
     */
    public int getFreePageCount() {
        return freePages.size();
    }

    /**
     * 获取下一个将要分配的新页号
     *
     * 注意:此方法不实际分配页,仅返回下一个新页号。
     * 如果有空闲页,分配时会优先使用空闲页而不是此页号。
     *
     * @return 下一个新页号
     */
    public int getNextPageId() {
        return nextPageId;
    }

    /**
     * 重置页管理器
     *
     * 清空所有分配和释放记录,用于测试或特殊场景。
     * 注意:不会删除元数据文件。
     */
    public void reset() {
        nextPageId = 0;
        freePages.clear();
        allocatedPages.clear();
    }

    /**
     * 删除元数据文件
     *
     * 用于测试或清理。
     */
    public void deleteMetadata() {
        if (tableId >= 0) {
            try {
                Path metaPath = getMetadataFilePath(tableId);
                if (Files.exists(metaPath)) {
                    Files.delete(metaPath);
                }
            } catch (IOException e) {
                throw new RuntimeException("Failed to delete metadata: tableId=" + tableId, e);
            }
        }
    }

    /**
     * 获取元数据文件路径
     *
     * @param tableId 表ID
     * @return 元数据文件路径
     */
    private Path getMetadataFilePath(int tableId) {
        return Path.of(DATA_DIR, "table_" + tableId + META_FILE_EXT);
    }

    @Override
    public String toString() {
        return "PageManager{" +
                "tableId=" + tableId +
                ", nextPageId=" + nextPageId +
                ", allocatedPages=" + allocatedPages.cardinality() +
                ", freePages=" + freePages.size() +
                '}';
    }
}
