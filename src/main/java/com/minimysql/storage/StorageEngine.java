package com.minimysql.storage;

import com.minimysql.storage.buffer.BufferPool;
import com.minimysql.storage.table.Column;
import com.minimysql.storage.table.Table;

import java.util.List;

/**
 * StorageEngine - 存储引擎接口
 *
 * 定义存储层的标准操作，所有存储引擎必须实现此接口。
 * 采用策略模式，支持多种存储引擎实现（InnoDB、Memory、CSV等）。
 *
 * MySQL存储引擎对应关系:
 * - StorageEngine接口 → MySQL的存储引擎抽象层（handler接口）
 * - InnoDBStorageEngine → MySQL的InnoDB存储引擎
 * - MemoryStorageEngine → MySQL的Memory存储引擎（预留）
 * - CSVStorageEngine → MySQL的CSV存储引擎（预留）
 *
 * 核心功能:
 * 1. 表管理:创建、获取、删除表
 * 2. 引擎生命周期:打开、关闭
 * 3. 元数据查询:检查表是否存在、获取所有表名
 * 4. BufferPool访问:用于元数据持久化等高级操作
 *
 * 设计原则:
 * - 接口隔离:只定义存储层必需的操作
 * - 引擎无关:上层Executor不需要关心具体引擎实现
 * - 可扩展:新增引擎只需实现此接口
 *
 * 使用模式:
 * <pre>
 * // 使用InnoDB引擎
 * StorageEngine engine = new InnoDBStorageEngine(1024);
 *
 * // 创建表（引擎无关）
 * Table users = engine.createTable("users", columns);
 *
 * // 关闭引擎
 * engine.close();
 * </pre>
 *
 * "Good taste": 所有引擎统一接口，没有特殊情况
 *
 * "实用主义": 只定义核心操作，不过度抽象
 */
public interface StorageEngine {

    /**
     * 创建表
     *
     * 创建新表，自动分配表ID，初始化索引结构。
     * 具体的索引策略由各引擎实现决定。
     *
     * @param tableName 表名
     * @param columns 列定义列表
     * @return 创建的Table实例
     * @throws IllegalArgumentException 表已存在或参数无效
     * @throws IllegalStateException 引擎已关闭
     */
    Table createTable(String tableName, List<Column> columns);

    /**
     * 获取表
     *
     * 根据表名获取已存在的表实例。
     *
     * @param tableName 表名
     * @return 表实例，如果不存在返回null
     */
    Table getTable(String tableName);

    /**
     * 删除表
     *
     * 删除表并释放相关资源。
     * 具体的删除行为由各引擎实现决定（如是否删除磁盘文件）。
     *
     * @param tableName 表名
     * @return 删除成功返回true，表不存在返回false
     */
    boolean dropTable(String tableName);

    /**
     * 创建索引
     *
     * 在已存在的表上创建二级索引。
     * 如果索引已存在，抛出异常。
     *
     * @param tableName 表名
     * @param indexName 索引名称
     * @param columnName 索引列名
     * @param unique 是否为唯一索引
     * @throws IllegalArgumentException 表不存在、列不存在、索引已存在
     */
    void createIndex(String tableName, String indexName, String columnName, boolean unique);

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
    boolean dropIndex(String tableName, String indexName);

    /**
     * 检查表是否存在
     *
     * @param tableName 表名
     * @return 存在返回true，否则返回false
     */
    boolean tableExists(String tableName);

    /**
     * 获取所有表名
     *
     * @return 表名列表
     */
    List<String> getAllTableNames();

    /**
     * 获取表的数量
     *
     * @return 表数量
     */
    int getTableCount();

    /**
     * 获取BufferPool
     *
     * 用于测试、监控和高级操作（如强制刷新系统表）。
     *
     * @return BufferPool实例
     */
    BufferPool getBufferPool();

    /**
     * 关闭存储引擎
     *
     * 释放所有资源，保存状态到磁盘。
     * 关闭后不能再创建或操作表。
     */
    void close();
}
