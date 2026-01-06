package com.minimysql.storage.page;

import java.util.HashSet;
import java.util.Set;

/**
 * PageManager - 页分配管理器
 *
 * PageManager负责管理表中页的分配和释放,类似内存分配器管理物理内存。
 *
 * 核心功能:
 * 1. 页分配:分配新的页号,优先重用已释放的页
 * 2. 页释放:释放不再使用的页号,供后续重用
 * 3. 页追踪:记录哪些页号正在使用
 *
 * 设计原则:
 * - 页号从0开始递增
 * - 释放的页号加入空闲集合,供后续重用
 * - 分配时优先使用空闲页,否则分配新页号
 * - 简单直接,没有复杂的段管理、区管理等
 *
 * 使用模式:
 * <pre>
 * PageManager manager = new PageManager();
 * int pageId = manager.allocatePage();  // 分配页号
 * bufferPool.newPage(tableId, pageId);  // 在缓冲池中创建页
 *
 * manager.freePage(pageId);  // 释放页号
 * </pre>
 *
 * 设计哲学:
 * - "Good taste": 没有特殊情况,分配和释放逻辑统一
 * - 实用主义: 释放页时立即标记为空闲,不延迟回收
 * - 简洁优先: 不实现复杂的页预分配、批量分配等
 */
public class PageManager {

    /** 下一个将要分配的新页号 */
    private int nextPageId;

    /** 空闲页集合(已释放的页号) */
    private final Set<Integer> freePages;

    /** 已分配页集合(用于验证和调试) */
    private final Set<Integer> allocatedPages;

    /**
     * 创建新的页管理器
     */
    public PageManager() {
        this.nextPageId = 0;
        this.freePages = new HashSet<>();
        this.allocatedPages = new HashSet<>();
    }

    /**
     * 分配一个新页号
     *
     * 分配策略:
     * 1. 如果有空闲页(之前释放的),优先重用
     * 2. 否则分配下一个新页号
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
        allocatedPages.add(pageId);

        return pageId;
    }

    /**
     * 释放页号
     *
     * 将页号标记为空闲,可以被后续分配重用。
     * 如果页号不存在或已被释放,静默忽略(实用主义)。
     *
     * @param pageId 要释放的页号
     */
    public void freePage(int pageId) {
        // 检查页是否已分配
        if (!allocatedPages.contains(pageId)) {
            // 页号不存在或已被释放,静默忽略
            return;
        }

        // 从已分配集合移除
        allocatedPages.remove(pageId);

        // 加入空闲集合
        freePages.add(pageId);
    }

    /**
     * 检查页号是否已分配
     *
     * @param pageId 页号
     * @return 如果页已分配返回true,否则返回false
     */
    public boolean isPageAllocated(int pageId) {
        return allocatedPages.contains(pageId);
    }

    /**
     * 获取已分配页的数量
     *
     * @return 已分配页数
     */
    public int getAllocatedPageCount() {
        return allocatedPages.size();
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
     */
    public void reset() {
        nextPageId = 0;
        freePages.clear();
        allocatedPages.clear();
    }

    @Override
    public String toString() {
        return "PageManager{" +
                "nextPageId=" + nextPageId +
                ", allocatedPages=" + allocatedPages.size() +
                ", freePages=" + freePages.size() +
                '}';
    }
}
