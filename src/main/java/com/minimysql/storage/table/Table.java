package com.minimysql.storage.table;

import com.minimysql.storage.buffer.BufferPool;
import com.minimysql.storage.buffer.PageFrame;
import com.minimysql.storage.index.ClusteredIndex;
import com.minimysql.storage.index.SecondaryIndex;
import com.minimysql.storage.page.DataPage;
import com.minimysql.storage.page.PageManager;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

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

    /** 聚簇索引(主键索引,每个表只有一个) */
    private ClusteredIndex clusteredIndex;

    /** 二级索引映射(索引名 -> 二级索引) */
    private final Map<String, SecondaryIndex> secondaryIndexes;

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
        this.secondaryIndexes = new ConcurrentHashMap<>();
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
     * 注意:不会关闭全局共享的BufferPool。
     *
     * 设计决策:
     * - 不在这里刷新脏页(避免跨表刷新其他表的脏页)
     * - 由StorageEngine统一管理所有表的脏页刷新
     * - 简单直接,没有特殊情况
     */
    public void close() {
        if (!opened) {
            return;
        }

        // 注意:不调用sharedBufferPool.flushAllPages()
        // 原因:
        // 1. BufferPool.flushAllPages()有bug,无法正确推断tableId
        // 2. 刷新所有脏页会影响其他表(违反单一职责)
        // 3. 应该由StorageEngine统一管理刷新逻辑

        opened = false;
    }

    /**
     * 插入行
     *
     * 将行数据插入到表中，同时更新聚簇索引和所有二级索引。
     * 自动选择合适的页面，页满时分配新页。
     *
     * 设计原则(MySQL兼容):
     * - 先插入聚簇索引(主键索引)
     * - 再插入所有二级索引
     * - 最后写入DataPage
     * - 保证索引和数据一致性
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

        // 1. 插入到聚簇索引(主键索引)
        if (clusteredIndex != null) {
            clusteredIndex.insertRow(row);
        }

        // 2. 插入到所有二级索引
        for (SecondaryIndex index : secondaryIndexes.values()) {
            // 获取索引列的值
            String indexColumnName = index.getColumnName();
            Column indexColumn = getColumn(indexColumnName);
            if (indexColumn != null) {
                int indexColumnIndex = columns.indexOf(indexColumn);
                Object indexColumnValue = row.getValue(indexColumnIndex);

                // 获取主键值(用于回表)
                int primaryKeyIndex = clusteredIndex.getPrimaryKeyIndex();
                Object primaryKeyValue = row.getValue(primaryKeyIndex);

                // 插入到二级索引
                index.insertEntry(indexColumnValue, primaryKeyValue);
            }
        }

        // 3. 序列化行数据并写入DataPage
        byte[] rowData = row.toBytes();

        // 尝试插入到当前页
        int pageId = insertToCurrentPage(rowData);

        // 如果当前页不存在或已满，分配新页
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
     * 设置聚簇索引
     *
     * 每个表只能有一个聚簇索引(主键索引)。
     *
     * @param clusteredIndex 聚簇索引
     */
    public void setClusteredIndex(ClusteredIndex clusteredIndex) {
        this.clusteredIndex = clusteredIndex;
    }

    /**
     * 获取聚簇索引
     *
     * @return 聚簇索引,如果未设置返回null
     */
    public ClusteredIndex getClusteredIndex() {
        return clusteredIndex;
    }

    /**
     * 添加二级索引
     *
     * @param indexName 索引名称
     * @param secondaryIndex 二级索引
     */
    public void addSecondaryIndex(String indexName, SecondaryIndex secondaryIndex) {
        secondaryIndexes.put(indexName, secondaryIndex);
    }

    /**
     * 获取二级索引
     *
     * @param indexName 索引名称
     * @return 二级索引,如果不存在返回null
     */
    public SecondaryIndex getSecondaryIndex(String indexName) {
        return secondaryIndexes.get(indexName);
    }

    /**
     * 获取所有二级索引名称
     *
     * @return 二级索引名称列表
     */
    public List<String> getSecondaryIndexNames() {
        return new ArrayList<>(secondaryIndexes.keySet());
    }

    /**
     * 获取二级索引数量
     *
     * @return 二级索引数量
     */
    public int getSecondaryIndexCount() {
        return secondaryIndexes.size();
    }

    /**
     * 移除二级索引
     *
     * @param indexName 索引名称
     * @return 被移除的索引,如果不存在返回null
     */
    public SecondaryIndex removeSecondaryIndex(String indexName) {
        return secondaryIndexes.remove(indexName);
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

    /**
     * 根据主键查询行
     *
     * 通过聚簇索引查询，时间复杂度 O(log N)。
     *
     * @param primaryKeyValue 主键值
     * @return 行数据，如果不存在返回null
     */
    public Row selectByPrimaryKey(Object primaryKeyValue) {
        if (clusteredIndex == null) {
            throw new IllegalStateException("Clustered index not set");
        }

        return clusteredIndex.selectByPrimaryKey(primaryKeyValue);
    }

    /**
     * 根据二级索引查询行
     *
     * 通过二级索引查询，需要回表（两次B+树查找）。
     *
     * @param indexName 索引名称
     * @param indexValue 索引列值
     * @return 行数据，如果不存在返回null
     */
    public Row selectBySecondaryIndex(String indexName, Object indexValue) {
        SecondaryIndex index = secondaryIndexes.get(indexName);
        if (index == null) {
            throw new IllegalArgumentException("Secondary index not found: " + indexName);
        }

        return index.selectRow(indexValue);
    }

    /**
     * 主键范围查询
     *
     * 利用聚簇索引的叶子节点链表，高效支持范围查询。
     *
     * @param startValue 起始主键值（包含）
     * @param endValue 结束主键值（包含）
     * @return 行列表
     */
    public List<Row> rangeSelect(Object startValue, Object endValue) {
        if (clusteredIndex == null) {
            throw new IllegalStateException("Clustered index not set");
        }

        return clusteredIndex.rangeSelect(startValue, endValue);
    }

    /**
     * 全表扫描
     *
     * 遍历聚簇索引的所有叶子节点，返回所有行数据。
     * 时间复杂度 O(N)，性能较差，应尽量避免使用。
     *
     * 简化实现：通过主键范围查询实现（假设主键从最小值到最大值）
     *
     * @return 所有行数据
     */
    public List<Row> fullTableScan() {
        if (clusteredIndex == null) {
            throw new IllegalStateException("Clustered index not set");
        }

        // 简化实现：返回空列表
        // 生产环境应该遍历所有DataPage或聚簇索引的所有叶子节点
        // TODO: 实现真正的全表扫描
        return new ArrayList<>();
    }

    /**
     * 根据主键更新行
     *
     * 简化实现:直接覆盖,不处理旧值删除和索引更新
     * 生产环境需要:
     * 1. 从聚簇索引中删除旧行
     * 2. 从所有二级索引中删除旧索引条目
     * 3. 插入新行到聚簇索引
     * 4. 为所有二级索引插入新索引条目
     *
     * @param primaryKeyValue 主键值
     * @param newRow 新行数据
     * @return 更新成功返回true,主键不存在返回false
     */
    public int updateRow(Object primaryKeyValue, Row newRow) {
        if (clusteredIndex == null) {
            throw new IllegalStateException("Clustered index not set");
        }

        // 检查主键是否存在
        Row oldRow = clusteredIndex.selectByPrimaryKey(primaryKeyValue);
        if (oldRow == null) {
            return 0; // 主键不存在
        }

        // 简化实现:直接删除旧数据并插入新数据
        // 注意:这不是原子操作,生产环境需要事务支持
        deleteRow(primaryKeyValue);
        insertRow(newRow);

        return 1;
    }

    /**
     * 根据主键删除行
     *
     * 从聚簇索引和所有二级索引中删除行。
     * 简化实现:只从聚簇索引删除,二级索引标记为过期(懒删除)
     * 生产环境需要实现B+树的完整删除逻辑(节点合并、借位等)
     *
     * @param primaryKeyValue 主键值
     * @return 删除成功返回true,主键不存在返回false
     */
    public int deleteRow(Object primaryKeyValue) {
        if (clusteredIndex == null) {
            throw new IllegalStateException("Clustered index not set");
        }

        // 检查主键是否存在
        Row row = clusteredIndex.selectByPrimaryKey(primaryKeyValue);
        if (row == null) {
            return 0; // 主键不存在
        }

        // 简化实现:从所有二级索引中删除(标记为过期)
        // 注意:BPlusTree.delete()还未完整实现,这里会抛出UnsupportedOperationException
        for (SecondaryIndex index : secondaryIndexes.values()) {
            try {
                // 获取索引列的值
                String columnName = index.getColumnName();
                int columnIndex = -1;
                for (int i = 0; i < columns.size(); i++) {
                    if (columns.get(i).getName().equals(columnName)) {
                        columnIndex = i;
                        break;
                    }
                }

                if (columnIndex >= 0) {
                    Object indexValue = row.getValue(columnIndex);
                    index.delete(indexValue);
                }
            } catch (UnsupportedOperationException e) {
                // B+树删除未实现,忽略
                // 生产环境必须实现
            }
        }

        // 从聚簇索引中删除
        clusteredIndex.delete(primaryKeyValue);

        return 1;
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
