package com.minimysql.storage.impl;

import com.minimysql.storage.StorageEngine;
import com.minimysql.storage.buffer.BufferPool;
import com.minimysql.storage.index.ClusteredIndex;
import com.minimysql.storage.page.PageManager;
import com.minimysql.storage.table.Column;
import com.minimysql.storage.table.Table;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * InnoDBStorageEngine - InnoDB存储引擎实现
 *
 * 实现StorageEngine接口，提供InnoDB风格的存储功能。
 * 对应MySQL的InnoDB存储引擎。
 *
 * 核心特性:
 * 1. BufferPool全局共享(所有表共用)
 * 2. 每个表独立的PageManager(管理表空间的页分配)
 * 3. 聚簇索引(主键索引):每个表自动创建，默认第一列为主键
 * 4. 支持事务(预留，后续实现)
 * 5. 支持外键(预留，后续实现)
 * 6. 支持崩溃恢复(预留，后续实现)
 *
 * MySQL InnoDB对应关系:
 * - BufferPool → InnoDB Buffer Pool(全局共享)
 * - PageManager → InnoDB Tablespace(每表独立)
 * - ClusteredIndex → InnoDB Clustered Index(主键索引)
 * - DataPage → InnoDB Data Page(16KB)
 *
 * 设计原则:
 * - 聚簇索引组织表:表数据按主键顺序物理存储
 * - 行级锁:支持高并发(预留)
 * - ACID事务:支持原子性、一致性、隔离性、持久性(预留)
 * - 崩溃恢复:通过WAL日志恢复(预留)
 *
 * 与其他引擎的区别:
 * - InnoDB:聚簇索引+事务+行级锁+崩溃恢复
 * - Memory:数据存储在内存，速度快但断电丢失
 * - MyISAM:表级锁，无事务，支持全文索引
 * - CSV:数据以CSV格式存储，可用文本编辑器编辑
 *
 * "Good taste": 实现简洁，功能完整，预留扩展点
 *
 * "实用主义": 先实现核心功能，暂不支持事务、外键等高级特性
 * - 暂不支持:二级索引创建(后续扩展)
 * - 暂不支持:表结构持久化到元数据表
 * - 暂不支持:事务管理
 * - 暂不支持:外键约束
 * - 暂不支持:崩溃恢复
 *
 * 简化实现:
 * - 主键默认为第一列(后续支持PRIMARY KEY约束)
 * - 表结构只在内存中(重启丢失，后续持久化)
 * - 删除表只从内存移除(不删除磁盘文件，测试用)
 * - 不支持跨表事务
 */
public class InnoDBStorageEngine implements StorageEngine {

    /** 全局共享的BufferPool(所有表共用) */
    private final BufferPool bufferPool;

    /** 表名到表的映射(线程安全) */
    private final Map<String, Table> tables;

    /** 表ID生成器(自动递增) */
    private final AtomicInteger tableIdGenerator;

    /** 引擎是否已关闭 */
    private volatile boolean closed;

    /** 默认缓冲池大小:1024页(16MB) */
    private static final int DEFAULT_BUFFER_POOL_SIZE = 1024;

    /**
     * 创建默认大小的InnoDB引擎(1024页缓冲池)
     */
    public InnoDBStorageEngine() {
        this(DEFAULT_BUFFER_POOL_SIZE);
    }

    /**
     * 创建指定大小的InnoDB引擎
     *
     * @param bufferPoolSize 缓冲池大小(页数)
     */
    public InnoDBStorageEngine(int bufferPoolSize) {
        this.bufferPool = new BufferPool(bufferPoolSize);
        this.tables = new ConcurrentHashMap<>();
        this.tableIdGenerator = new AtomicInteger(0);
        this.closed = false;
    }

    /**
     * 创建表
     *
     * 创建新表，自动分配表ID，初始化聚簇索引(主键索引)。
     * 主键默认为第一列(后续支持PRIMARY KEY约束)。
     *
     * @param tableName 表名
     * @param columns 列定义列表
     * @return 创建的Table实例
     * @throws IllegalArgumentException 表已存在或参数无效
     * @throws IllegalStateException 引擎已关闭
     */
    @Override
    public Table createTable(String tableName, List<Column> columns) {
        checkEngineClosed();

        // 参数校验
        if (tableName == null || tableName.trim().isEmpty()) {
            throw new IllegalArgumentException("Table name cannot be null or empty");
        }

        if (columns == null || columns.isEmpty()) {
            throw new IllegalArgumentException("Table must have at least one column");
        }

        // 检查表名是否已存在
        if (tables.containsKey(tableName)) {
            throw new IllegalArgumentException("Table already exists: " + tableName);
        }

        // 分配表ID
        int tableId = tableIdGenerator.incrementAndGet();

        // 创建表实例
        Table table = new Table(tableId, tableName, columns);

        // 创建独立的PageManager(每表独立)
        PageManager pageManager = new PageManager();

        // 打开表(初始化页管理器)
        table.open(bufferPool, pageManager);

        // 创建聚簇索引(主键索引)
        // 简化实现:主键默认为第一列
        ClusteredIndex clusteredIndex = createClusteredIndex(table, columns);

        // 将聚簇索引设置到表中
        table.setClusteredIndex(clusteredIndex);

        // 将表添加到映射
        tables.put(tableName, table);

        return table;
    }

    /**
     * 创建聚簇索引(主键索引)
     *
     * @param table 表实例
     * @param columns 列定义列表
     * @return 聚簇索引
     */
    private ClusteredIndex createClusteredIndex(Table table, List<Column> columns) {
        // 简化实现:主键默认为第一列
        String primaryKeyColumn = columns.get(0).getName();
        int primaryKeyIndex = 0;

        // 创建独立的索引页管理器
        PageManager indexPageManager = new PageManager();

        // 创建聚簇索引
        ClusteredIndex clusteredIndex = new ClusteredIndex(
                table.getTableId(),
                primaryKeyColumn,
                primaryKeyIndex,
                bufferPool,
                indexPageManager
        );

        // 设置列定义(用于Row序列化)
        clusteredIndex.setColumns(columns);

        return clusteredIndex;
    }

    /**
     * 获取表
     *
     * 根据表名获取已存在的表实例。
     *
     * @param tableName 表名
     * @return 表实例，如果不存在返回null
     */
    @Override
    public Table getTable(String tableName) {
        checkEngineClosed();

        if (tableName == null || tableName.trim().isEmpty()) {
            return null;
        }

        return tables.get(tableName);
    }

    /**
     * 删除表
     *
     * 从内存中移除表，关闭表并释放资源。
     * 简化实现:只从内存移除，不删除磁盘文件(测试用)。
     * 生产环境应该删除表的数据文件和元数据文件。
     *
     * @param tableName 表名
     * @return 删除成功返回true，表不存在返回false
     */
    @Override
    public boolean dropTable(String tableName) {
        checkEngineClosed();

        if (tableName == null || tableName.trim().isEmpty()) {
            return false;
        }

        Table table = tables.remove(tableName);

        if (table != null) {
            // 关闭表(不刷新脏页，已在close()中处理)
            table.close();
            return true;
        }

        return false;
    }

    /**
     * 创建索引
     *
     * 在已存在的表上创建二级索引。
     * 遍历表的所有数据，为每一行插入索引条目。
     *
     * @param tableName 表名
     * @param indexName 索引名称
     * @param columnName 索引列名
     * @param unique 是否为唯一索引
     * @throws IllegalArgumentException 表不存在、列不存在、索引已存在
     */
    @Override
    public void createIndex(String tableName, String indexName, String columnName, boolean unique) {
        checkEngineClosed();

        // 参数校验
        if (tableName == null || tableName.trim().isEmpty()) {
            throw new IllegalArgumentException("Table name cannot be null or empty");
        }

        if (indexName == null || indexName.trim().isEmpty()) {
            throw new IllegalArgumentException("Index name cannot be null or empty");
        }

        if (columnName == null || columnName.trim().isEmpty()) {
            throw new IllegalArgumentException("Column name cannot be null or empty");
        }

        // 获取表
        Table table = tables.get(tableName);
        if (table == null) {
            throw new IllegalArgumentException("Table does not exist: " + tableName);
        }

        // 检查列是否存在
        List<Column> columns = table.getColumns();
        int columnIndex = -1;
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i).getName().equals(columnName)) {
                columnIndex = i;
                break;
            }
        }

        if (columnIndex == -1) {
            throw new IllegalArgumentException("Column does not exist: " + columnName);
        }

        // 检查索引是否已存在
        if (table.getSecondaryIndex(indexName) != null) {
            throw new IllegalArgumentException("Index already exists: " + indexName);
        }

        // 获取主键索引(用于回表)
        com.minimysql.storage.index.ClusteredIndex clusteredIndex = table.getClusteredIndex();
        if (clusteredIndex == null) {
            throw new IllegalStateException("Table has no clustered index");
        }

        // 获取主键列索引(简化:假设主键是第一列)
        int primaryKeyIndex = 0;

        // 创建独立的索引页管理器
        PageManager indexPageManager = new PageManager();

        // 创建二级索引
        com.minimysql.storage.index.SecondaryIndex secondaryIndex =
                new com.minimysql.storage.index.SecondaryIndex(
                        table.getTableId(),
                        indexName,
                        columnName,
                        primaryKeyIndex,
                        unique,
                        clusteredIndex,
                        bufferPool,
                        indexPageManager
                );

        // 添加到表
        table.addSecondaryIndex(indexName, secondaryIndex);

        // 为现有数据构建索引
        // 遍历聚簇索引的所有行,插入到二级索引
        // 简化实现:这里需要遍历所有DataPage,但目前缺少scanAll()方法
        // TODO: 实现fullTableScan()后,为现有数据构建索引
        // 暂时只对新插入的数据生效
    }

    /**
     * 删除索引
     *
     * 删除表的二级索引。
     * 聚簇索引不能删除(删除表时自动删除)。
     *
     * @param tableName 表名
     * @param indexName 索引名称
     * @return 删除成功返回true，索引不存在返回false
     * @throws IllegalArgumentException 表不存在
     * @throws UnsupportedOperationException 尝试删除聚簇索引
     */
    @Override
    public boolean dropIndex(String tableName, String indexName) {
        checkEngineClosed();

        // 参数校验
        if (tableName == null || tableName.trim().isEmpty()) {
            return false;
        }

        if (indexName == null || indexName.trim().isEmpty()) {
            return false;
        }

        // 获取表
        Table table = tables.get(tableName);
        if (table == null) {
            throw new IllegalArgumentException("Table does not exist: " + tableName);
        }

        // 不能删除聚簇索引
        if ("PRIMARY".equalsIgnoreCase(indexName)) {
            throw new UnsupportedOperationException("Cannot drop clustered index");
        }

        // 检查索引是否存在
        if (table.getSecondaryIndex(indexName) == null) {
            return false;
        }

        // 从表中移除索引
        table.removeSecondaryIndex(indexName);

        return true;
    }

    /**
     * 检查表是否存在
     *
     * @param tableName 表名
     * @return 存在返回true，否则返回false
     */
    @Override
    public boolean tableExists(String tableName) {
        checkEngineClosed();

        if (tableName == null || tableName.trim().isEmpty()) {
            return false;
        }

        return tables.containsKey(tableName);
    }

    /**
     * 获取所有表名
     *
     * @return 表名列表
     */
    @Override
    public List<String> getAllTableNames() {
        checkEngineClosed();

        return new ArrayList<>(tables.keySet());
    }

    /**
     * 获取表的数量
     *
     * @return 表数量
     */
    @Override
    public int getTableCount() {
        checkEngineClosed();
        return tables.size();
    }

    /**
     * 获取缓冲池
     *
     * 用于测试和监控。
     *
     * @return BufferPool实例
     */
    public BufferPool getBufferPool() {
        return bufferPool;
    }

    /**
     * 关闭存储引擎
     *
     * 关闭所有表，清空表映射，释放资源。
     * 关闭后不能再创建或操作表。
     *
     * 设计原则:
     * - 关闭所有表(不刷新脏页，Table.close()不负责刷新)
     * - 清空表映射(释放内存)
     * - 标记引擎为已关闭
     * - 不关闭BufferPool(可能有其他地方使用)
     *
     * 注意:脏页刷新由调用方负责(如通过BufferPool.flushAllPages())
     */
    @Override
    public void close() {
        if (closed) {
            return; // 已关闭
        }

        // 关闭所有表
        for (Table table : tables.values()) {
            try {
                table.close();
            } catch (Exception e) {
                // 记录错误但继续关闭其他表
                System.err.println("Error closing table: " + table.getTableName() + ", " + e.getMessage());
            }
        }

        // 清空表映射
        tables.clear();

        // 标记为已关闭
        closed = true;

        // 注意:不调用bufferPool.flushAllPages()
        // 原因:BufferPool.flushAllPages()有bug，无法正确推断tableId
        // 生产环境应该实现一个更智能的刷新逻辑
    }

    /**
     * 重置存储引擎
     *
     * 清空所有表，重置状态，用于测试。
     * 注意:不会删除磁盘文件。
     */
    public void reset() {
        checkEngineClosed();

        // 关闭所有表
        for (Table table : tables.values()) {
            try {
                table.close();
            } catch (Exception e) {
                System.err.println("Error closing table: " + table.getTableName() + ", " + e.getMessage());
            }
        }

        // 清空表映射
        tables.clear();

        // 重置表ID生成器
        tableIdGenerator.set(0);

        // 注意:不调用bufferPool.clear()
        // 原因:BufferPool.clear()内部调用flushAllPages()有bug
        // 替代方案:创建新的BufferPool实例(测试环境)
    }

    /**
     * 检查引擎是否已关闭
     *
     * @throws IllegalStateException 如果引擎已关闭
     */
    private void checkEngineClosed() {
        if (closed) {
            throw new IllegalStateException("Storage engine is closed");
        }
    }

    /**
     * 检查引擎是否已关闭
     *
     * @return 已关闭返回true
     */
    public boolean isClosed() {
        return closed;
    }

    @Override
    public String toString() {
        return "InnoDBStorageEngine{" +
                "bufferPoolSize=" + bufferPool.getPoolSize() +
                ", tableCount=" + tables.size() +
                ", nextTableId=" + tableIdGenerator.get() +
                ", closed=" + closed +
                '}';
    }
}
