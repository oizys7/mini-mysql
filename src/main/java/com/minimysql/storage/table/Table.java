package com.minimysql.storage.table;

import com.minimysql.storage.buffer.BufferPool;
import com.minimysql.storage.buffer.PageFrame;
import com.minimysql.storage.page.DataPage;
import com.minimysql.storage.page.PageManager;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Table - 表定义
 *
 * Table定义了表的结构和基本操作,是存储层的核心接口。
 *
 * 核心功能:
 * 1. 表结构定义:列定义、表名、表ID
 * 2. 行数据插入:将Row存储到DataPage中
 * 3. 页管理:通过PageManager分配页号
 * 4. 缓存管理:使用全局共享的BufferPool
 *
 * 设计原则(MySQL兼容):
 * - 表结构不可变(创建后列定义不可修改)
 * - BufferPool全局共享,所有表共用(符合MySQL InnoDB设计)
 * - PageManager每表独立(管理表空间的页分配)
 * - 插入操作自动分配新页,页满时分配新页
 * - 暂不支持主键约束(后续扩展)
 *
 * MySQL InnoDB对应关系:
 * - Table → InnoDB Table
 * - BufferPool → InnoDB Buffer Pool(全局共享)
 * - PageManager → InnoDB Table Space(每表独立)
 * - DataPage → InnoDB Data Page
 *
 * 使用模式:
 * <pre>
 * // 创建全局BufferPool(所有表共享)
 * BufferPool globalBufferPool = new BufferPool(1024);
 *
 * // 创建表
 * List<Column> columns = Arrays.asList(
 *     new Column("id", DataType.INT, false),
 *     new Column("name", DataType.VARCHAR, 100, true),
 *     new Column("age", DataType.INT, false)
 * );
 *
 * Table table = new Table(tableId, "users", columns);
 * table.open(globalBufferPool, pageManager);
 *
 * // 插入行
 * Object[] values = {1, "Alice", 25};
 * Row row = new Row(columns, values);
 * table.insertRow(row);
 * </pre>
 *
 * 设计哲学:
 * - "Good taste": 表结构清晰,没有隐藏的复杂性
 * - 实用主义: 先实现基本功能,暂不支持删除和更新
 * - MySQL原理: BufferPool全局共享,与MySQL InnoDB一致
 * - 简洁优先: 不实现复杂的页分裂、合并等
 */
public class Table {

    /** 表ID (对应MySQL的space_id) */
    private final int tableId;

    /** 表名 */
    private final String tableName;

    /** 列定义列表 */
    private final List<Column> columns;

    /** 页管理器 (每表独立,对应MySQL的Table Space) */
    private PageManager pageManager;

    /** 全局共享的BufferPool引用 (不拥有,仅使用) */
    private BufferPool sharedBufferPool;

    /** 当前页号(用于插入) */
    private int currentPageId;

    /** 表是否已打开 */
    private boolean opened;

    /**
     * 创建表定义
     *
     * @param tableId 表ID
     * @param tableName 表名
     * @param columns 列定义列表
     */
    public Table(int tableId, String tableName, List<Column> columns) {
        if (tableName == null || tableName.trim().isEmpty()) {
            throw new IllegalArgumentException("Table name cannot be null or empty");
        }

        if (columns == null || columns.isEmpty()) {
            throw new IllegalArgumentException("Table must have at least one column");
        }

        // 检查列名重复
        for (int i = 0; i < columns.size(); i++) {
            for (int j = i + 1; j < columns.size(); j++) {
                if (columns.get(i).getName().equalsIgnoreCase(columns.get(j).getName())) {
                    throw new IllegalArgumentException("Duplicate column name: " +
                            columns.get(i).getName());
                }
            }
        }

        this.tableId = tableId;
        this.tableName = tableName;
        this.columns = new ArrayList<>(columns); // defensive copy
        this.currentPageId = -1;
        this.opened = false;
    }

    /**
     * 打开表
     *
     * 初始化页管理器,引用全局BufferPool,加载表状态。
     * 注意:BufferPool是全局共享的,不会被Table拥有或关闭。
     *
     * @param sharedBufferPool 全局共享的缓冲池
     * @param pageManager 页管理器(每表独立)
     */
    public void open(BufferPool sharedBufferPool, PageManager pageManager) {
        if (opened) {
            return; // 已打开
        }

        this.sharedBufferPool = sharedBufferPool;
        this.pageManager = pageManager;

        // 加载页管理器状态
        this.pageManager.load(tableId);

        // 查找最后一页(用于插入)
        this.currentPageId = pageManager.getNextPageId() - 1;

        this.opened = true;
    }

    /**
     * 关闭表
     *
     * 释放资源,保存状态。
     * 注意:不会关闭全局共享的BufferPool,只刷新本表的脏页。
     */
    public void close() {
        if (!opened) {
            return;
        }

        // 刷新本表的脏页到磁盘
        if (sharedBufferPool != null) {
            // 注意:这里会刷新BufferPool中所有脏页,包括其他表的
            // 简化实现:全局flush(生产环境应该只flush本表的页)
            sharedBufferPool.flushAllPages();
        }

        opened = false;
    }

    /**
     * 插入行
     *
     * 将行数据插入到表中。
     * 自动选择合适的页面,页满时分配新页。
     *
     * @param row 行数据
     * @return 插入的页号
     */
    public int insertRow(Row row) {
        if (!opened) {
            throw new IllegalStateException("Table is not opened");
        }

        if (row == null) {
            throw new IllegalArgumentException("Row cannot be null");
        }

        // 序列化行数据
        byte[] rowData = row.toBytes();

        // 尝试插入到当前页
        int pageId = insertToCurrentPage(rowData);

        // 如果当前页不存在或已满,分配新页
        if (pageId == -1) {
            pageId = allocateNewPage();
            insertToPage(pageId, rowData);
        }

        return pageId;
    }

    /**
     * 获取表ID
     */
    public int getTableId() {
        return tableId;
    }

    /**
     * 获取表名
     */
    public String getTableName() {
        return tableName;
    }

    /**
     * 获取列定义列表
     */
    public List<Column> getColumns() {
        return new ArrayList<>(columns); // defensive copy
    }

    /**
     * 获取列数
     */
    public int getColumnCount() {
        return columns.size();
    }

    /**
     * 根据列名查找列定义
     *
     * @param columnName 列名
     * @return 列定义,如果不存在返回null
     */
    public Column getColumn(String columnName) {
        for (Column col : columns) {
            if (col.getName().equalsIgnoreCase(columnName)) {
                return col;
            }
        }
        return null;
    }

    /**
     * 尝试插入到当前页
     *
     * @param rowData 行数据
     * @return 成功返回页号,失败返回-1
     */
    private int insertToCurrentPage(byte[] rowData) {
        if (currentPageId < 0) {
            return -1;
        }

        PageFrame frame = sharedBufferPool.getPage(tableId, currentPageId);
        DataPage page = (DataPage) frame.getPage();

        if (!page.hasFreeSpace(rowData.length)) {
            return -1;
        }

        frame.pin();
        try {
            page.insertRow(rowData);
            frame.markDirty();
            return currentPageId;
        } finally {
            frame.unpin(false);
        }
    }

    /**
     * 分配新页
     *
     * @return 新页号
     */
    private int allocateNewPage() {
        int newPageId = pageManager.allocatePage();
        PageFrame frame = sharedBufferPool.newPage(tableId, newPageId);

        frame.pin();
        frame.unpin(false);

        currentPageId = newPageId;
        return newPageId;
    }

    /**
     * 插入到指定页
     *
     * @param pageId 页号
     * @param rowData 行数据
     */
    private void insertToPage(int pageId, byte[] rowData) {
        PageFrame frame = sharedBufferPool.getPage(tableId, pageId);
        DataPage page = (DataPage) frame.getPage();

        frame.pin();
        try {
            page.insertRow(rowData);
            frame.markDirty();
        } finally {
            frame.unpin(false);
        }
    }

    @Override
    public String toString() {
        return "Table{" +
                "tableId=" + tableId +
                ", tableName='" + tableName + '\'' +
                ", columns=" + columns.size() +
                ", opened=" + opened +
                '}';
    }
}
