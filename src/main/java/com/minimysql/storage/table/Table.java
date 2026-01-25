package com.minimysql.storage.table;

import com.minimysql.storage.buffer.BufferPool;
import com.minimysql.storage.buffer.PageFrame;
import com.minimysql.storage.index.ClusteredIndex;
import com.minimysql.storage.index.SecondaryIndex;
import com.minimysql.storage.page.DataPage;
import com.minimysql.storage.page.PageManager;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Table - 表定义 (Table Metadata)
 *
 * <p>Table 定义了表的结构和元数据，对应 MySQL 的表定义 (Information Schema)。
 *
 * <p>核心功能:
 * <ul>
 *   <li>表结构定义:列定义、表名、表ID</li>
 *   <li>索引管理:聚簇索引、二级索引</li>
 *   <li>行数据操作:插入、查询、更新、删除 (委托给 ClusteredIndex)</li>
 *   <li>页管理:通过 PageManager 分配页号</li>
 *   <li>缓存管理:使用全局共享的 BufferPool</li>
 * </ul>
 *
 * <p>设计原则 (MySQL 兼容):
 * <ul>
 *   <li>表结构不可变 (创建后列定义不可修改)</li>
 *   <li>BufferPool 全局共享，所有表共用 (符合 MySQL InnoDB 设计)</li>
 *   <li>PageManager 每表独立 (管理表空间的页分配)</li>
 *   <li>插入操作自动分配新页，页满时分配新页</li>
 *   <li>聚簇索引 = 表 (Clustered Index = Table)</li>
 * </ul>
 *
 * <p>MySQL InnoDB 对应关系:
 * <ul>
 *   <li>Table → InnoDB Table Definition (元数据)</li>
 *   <li>BufferPool → InnoDB Buffer Pool (全局共享)</li>
 *   <li>PageManager → InnoDB Tablespace (每表独立)</li>
 *   <li>DataPage → InnoDB Data Page</li>
 *   <li>ClusteredIndex → InnoDB Clustered Index (表数据组织)</li>
 * </ul>
 *
 * <p>使用模式:
 * <pre>
 * // 创建全局 BufferPool (所有表共享)
 * BufferPool globalBufferPool = new BufferPool(1024);
 *
 * // 创建表
 * List&lt;Column&gt; columns = Arrays.asList(
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
 * Row row = new Row(values);
 * table.insertRow(row);
 * </pre>
 *
 * <p>设计哲学 (重构后):
 * <ul>
 *   <li>"Good taste": 表结构清晰，没有隐藏的复杂性</li>
 *   <li>职责单一: Table 只管理元数据，不处理物理序列化</li>
 *   <li>物理存储委托: 序列化/反序列化由 RecordSerializer 处理</li>
 *   <li>MySQL 原理: BufferPool 全局共享，与 MySQL InnoDB 一致</li>
 *   <li>简洁优先: 不实现复杂的页分裂、合并等</li>
 * </ul>
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
     *
     * TODO 检查关闭表的逻辑是否正确
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
     * <p>将行数据插入到表中，同时更新聚簇索引和所有二级索引。
     *
     * <p>设计原则 (MySQL 兼容):
     * <ul>
     *   <li>先插入聚簇索引 (主键索引) - 聚簇索引 = 表，数据存储在聚簇索引中</li>
     *   <li>再插入所有二级索引</li>
     *   <li>不再写入 DataPage (聚簇索引已经负责存储)</li>
     *   <li>保证索引和数据一致性</li>
     * </ul>
     *
     * <p>重构说明:
     * <ul>
     *   <li>移除了对 DataPage 的写入 (避免重复存储)</li>
     *   <li>聚簇索引已经负责物理存储 (使用 RecordSerializer 序列化)</li>
     *   <li>DataPage 将在未来用于溢出页 (大字段溢出)</li>
     * </ul>
     *
     * @param row 逻辑行数据
     * @return 插入的页号 (来自聚簇索引)
     */
    public int insertRow(Row row) {
        if (!opened) {
            throw new IllegalStateException("Table is not opened");
        }

        if (row == null) {
            throw new IllegalArgumentException("Row cannot be null");
        }

        // ✅ 重构后: 在Table中验证行数据 (Row不持有Column引用,无法验证)
        validateRow(row);

        // 1. 插入到聚簇索引 (主键索引)
        // 聚簇索引 = 表，数据存储在聚簇索引中
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

                // 获取主键值 (用于回表)
                int primaryKeyIndex = clusteredIndex.getPrimaryKeyIndex();
                Object primaryKeyValue = row.getValue(primaryKeyIndex);

                // 插入到二级索引
                index.insertEntry(indexColumnValue, primaryKeyValue);
            }
        }

        // 重构设计: 不再写入 DataPage
        // 原因: 聚簇索引已经负责物理存储 (通过 RecordSerializer 序列化)
        // DataPage 将在未来用于溢出页 (大字段溢出)

        return 0; // 返回值不再有意义 (数据存储在聚簇索引中)
    }

    /**
     * 验证行数据是否符合表约束
     *
     * <p>重构后: 验证逻辑从Row移到Table,因为Row不持有Column引用
     *
     * @param row 行数据
     * @throws IllegalArgumentException 如果行数据违反约束
     */
    private void validateRow(Row row) {
        if (row.getColumnCount() != columns.size()) {
            throw new IllegalArgumentException(
                    "Row column count (" + row.getColumnCount() +
                    ") does not match table column count (" + columns.size() + ")");
        }

        // 验证每一列
        for (int i = 0; i < columns.size(); i++) {
            Column col = columns.get(i);
            Object value = row.getValue(i);

            // 使用Column的validate方法
            if (!col.validate(value)) {
                if (value == null) {
                    throw new IllegalArgumentException(
                            "Column '" + col.getName() + "' cannot be NULL");
                } else {
                    throw new IllegalArgumentException(
                            "Column '" + col.getName() + "' value '" + value +
                            "' does not match type " + col.getType());
                }
            }
        }
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
     * 获取行中指定列的值
     *
     * <p>重构后推荐使用此方法替代已废弃的 Row.getValue(String)
     *
     * @param row       行数据
     * @param columnName 列名
     * @return 列值,如果列不存在返回null
     */
    public Object getRowValue(Row row, String columnName) {
        Column col = getColumn(columnName);
        if (col == null) {
            return null;
        }
        int index = columns.indexOf(col);
        return row.getValue(index);
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
     * 根据主键查询行
     *
     * <p>通过聚簇索引查询，时间复杂度 O(log N)。
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
     * <p>遍历聚簇索引的所有叶子节点，返回所有行数据。
     * 时间复杂度 O(N)，性能较差，应尽量避免使用。
     *
     * <p>实现: 通过聚簇索引的 {@link ClusteredIndex#getAll()} 方法遍历所有叶子节点
     *
     * <p>MySQL InnoDB 对应:
     * <ul>
     *   <li>全表扫描 → 扫描聚簇索引的所有叶子节点</li>
     *   <li>顺序 I/O → 叶子节点通过链表连接，顺序读取效率高</li>
     * </ul>
     *
     * <p>重构设计: 直接使用 {@link ClusteredIndex#getAllRows()} 返回的 Row 列表
     *
     * @return 所有逻辑行数据
     * @throws IllegalStateException 如果聚簇索引未设置
     */
    public List<Row> fullTableScan() {
        if (clusteredIndex == null) {
            throw new IllegalStateException("Clustered index not set");
        }

        // 直接从聚簇索引获取所有行 (已经是反序列化后的 Row)
        return clusteredIndex.getAllRows();
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
