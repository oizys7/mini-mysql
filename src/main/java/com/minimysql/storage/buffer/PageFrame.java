package com.minimysql.storage.buffer;

import com.minimysql.storage.page.Page;

/**
 * PageFrame - 页帧
 *
 * 页帧是缓冲池中的基本单位,封装了页数据及其元数据。
 *
 * 页帧包含四个关键元数据:
 * - page: 页数据(DataPage或IndexPage等)
 * - tableId: 表ID(用于刷新脏页到正确的表文件)
 * - dirty: 脏标记,表示页是否被修改过
 * - pinCount: 引用计数,表示有多少操作正在使用此页
 *
 * 设计哲学:
 * - pinCount > 0的页不能被淘汰(正在被使用)
 * - dirty的页在淘汰前必须写回磁盘
 * - pin/unpin必须配对,类似对象的引用计数
 * - tableId用于修复BufferPool.flushAllPages()的bug
 *
 * "Good taste": 没有锁状态、读写状态等复杂概念,只有dirty和pinCount
 */
public class PageFrame {

    /** 页数据 */
    private Page page;

    /** 表ID(用于脏页刷新到正确的表文件) */
    private int tableId;

    /** 脏标记:页内容是否被修改过 */
    private boolean dirty;

    /** 引用计数:有多少操作正在使用此页 */
    private int pinCount;

    /**
     * 创建页帧
     *
     * @param page 页数据
     */
    public PageFrame(Page page) {
        this.page = page;
        this.tableId = -1;  // 默认-1,表示未设置
        this.dirty = false;
        this.pinCount = 0;
    }

    /**
     * 获取页数据
     */
    public Page getPage() {
        return page;
    }

    /**
     * 设置页数据
     *
     * 仅在从磁盘读取页后调用。
     */
    public void setPage(Page page) {
        this.page = page;
    }

    /**
     * 获取表ID
     *
     * @return 表ID
     */
    public int getTableId() {
        return tableId;
    }

    /**
     * 设置表ID
     *
     * 在创建页帧时设置,用于将脏页刷新到正确的表文件。
     *
     * @param tableId 表ID
     */
    public void setTableId(int tableId) {
        this.tableId = tableId;
    }

    /**
     * 页是否为脏页
     *
     * 脏页表示页内容被修改过,与磁盘上的版本不一致。
     * 脏页在淘汰前必须写回磁盘。
     */
    public boolean isDirty() {
        return dirty;
    }

    /**
     * 标记页为脏页
     *
     * 当页内容被修改时调用。
     */
    public void markDirty() {
        this.dirty = true;
    }

    /**
     * 清除脏标记
     *
     * 在页写回磁盘后调用,表示页与磁盘一致。
     */
    public void clearDirty() {
        this.dirty = false;
    }

    /**
     * 获取引用计数
     *
     * 引用计数表示有多少操作正在使用此页。
     * pinCount > 0时页不能被淘汰。
     */
    public int getPinCount() {
        return pinCount;
    }

    /**
     * 增加引用计数(pin)
     *
     * 当操作开始使用页时调用。
     * 例如:扫描操作访问页、索引查找访问页。
     *
     * pin后必须配对调用unpin,否则页永远不会被淘汰。
     */
    public void pin() {
        this.pinCount++;
    }

    /**
     * 减少引用计数(unpin)
     *
     * 当操作结束使用页时调用。
     *
     * @param dirty 页是否被修改(如果被修改,标记为脏页)
     */
    public void unpin(boolean dirty) {
        if (this.pinCount <= 0) {
            throw new IllegalStateException("Cannot unpin a page with pinCount <= 0");
        }

        this.pinCount--;

        if (dirty) {
            this.dirty = true;
        }
    }

    /**
     * 页是否可以被淘汰
     *
     * 淘汰条件:引用计数为0(没有操作正在使用)
     */
    public boolean isEvictable() {
        return pinCount == 0;
    }

    @Override
    public String toString() {
        return "PageFrame{" +
                "pageId=" + (page != null ? page.getPageId() : "null") +
                ", dirty=" + dirty +
                ", pinCount=" + pinCount +
                '}';
    }
}
