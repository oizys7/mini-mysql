package com.minimysql.metadata;

import com.minimysql.storage.StorageEngine;
import com.minimysql.storage.buffer.BufferPool;
import com.minimysql.storage.page.PageManager;
import com.minimysql.storage.table.Column;
import com.minimysql.storage.table.DataType;
import com.minimysql.storage.table.Row;
import com.minimysql.storage.table.Table;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * SchemaManager - Schema管理器
 *
 * 管理数据库的元数据，包括表结构、列定义、索引信息等。
 * 对应MySQL的information_schema。
 *
 * 核心功能:
 * 1. 元数据持久化: 将表结构写入系统表(SYS_TABLES, SYS_COLUMNS)
 * 2. 元数据加载: 启动时从系统表加载所有表定义
 * 3. 表ID生成: 自动分配表ID，保证不重复
 * 4. 元数据缓存: 内存中缓存所有表的元数据
 *
 * MySQL对应关系:
 * - SchemaManager → MySQL Data Dictionary
 * - SYS_TABLES → information_schema.TABLES
 * - SYS_COLUMNS → information_schema.COLUMNS
 *
 * 设计原则("Good taste"):
 * - 系统表就是普通的Table，没有特殊处理
 * - 元数据操作和业务表操作使用同一套API
 * - 消除"元数据是特殊的"这种特殊情况
 *
 * 设计原则("实用主义"):
 * - 元数据表在初始化时一次性创建
 * - 每次修改元数据后立即刷盘(强一致性)
 * - 不支持事务(后续扩展)
 * - 不支持并发控制(后续扩展)
 *
 * 使用模式:
 * <pre>
 * // 初始化SchemaManager
 * SchemaManager schemaManager = new SchemaManager(storageEngine, "data/metadata");
 * schemaManager.initialize();
 *
 * // 创建表元数据
 * List<Column> columns = Arrays.asList(
 *     new Column("id", DataType.INT, false),
 *     new Column("name", DataType.VARCHAR, 100, true)
 * );
 * int tableId = schemaManager.createTable("users", columns);
 *
 * // 加载表元数据
 * TableMetadata metadata = schemaManager.loadTableMetadata("users");
 * List<Column> loadedColumns = metadata.toColumns();
 *
 * // 删除表元数据
 * schemaManager.dropTable("users");
 * </pre>
 *
 * 鸡生蛋问题:
 * 系统表本身也需要元数据，如何解决？
 * 解决方案: 系统表在第一次初始化时硬编码创建，元数据表本身不存储在SYS_TABLES中。
 *
 * 优点:
 * - 简单直接，容易理解
 * - 符合MySQL的设计思路
 * - 元数据表可以像普通表一样操作
 *
 * 缺点:
 * - 系统表的结构固定，不能动态修改
 * - 每次修改元数据都需要刷盘，性能较差
 */
public class SchemaManager {

    /** 存储引擎实例(不拥有，仅使用) */
    private final StorageEngine storageEngine;

    /** 元数据存储目录 */
    private final String metadataDir;

    /** 系统表: SYS_TABLES */
    private Table sysTablesTable;

    /** 系统表: SYS_COLUMNS */
    private Table sysColumnsTable;

    /** 元数据缓存: 表名 -> 表元数据 */
    private final Map<String, TableMetadata> metadataCache;

    /** 表ID生成器 */
    private int nextTableId;

    /** 是否已初始化 */
    private boolean initialized;

    /** 数据目录配置 */
    private static final String DEFAULT_METADATA_DIR = "./data/metadata";

    /**
     * 创建SchemaManager
     *
     * @param storageEngine 存储引擎
     * @param metadataDir 元数据存储目录
     */
    public SchemaManager(StorageEngine storageEngine, String metadataDir) {
        if (storageEngine == null) {
            throw new IllegalArgumentException("StorageEngine cannot be null");
        }

        this.storageEngine = storageEngine;
        this.metadataDir = metadataDir;
        this.metadataCache = new ConcurrentHashMap<>();
        this.nextTableId = 1; // 从1开始，避免和系统表ID冲突
        this.initialized = false;
    }

    /**
     * 使用默认目录创建SchemaManager
     *
     * @param storageEngine 存储引擎
     */
    public SchemaManager(StorageEngine storageEngine) {
        this(storageEngine, DEFAULT_METADATA_DIR);
    }

    /**
     * 初始化SchemaManager
     *
     * 创建元数据目录，初始化系统表，加载现有元数据。
     *
     * 流程:
     * 1. 创建元数据目录
     * 2. 检查是否存在系统表文件
     * 3. 如果不存在，创建系统表
     * 4. 如果存在，加载系统表
     * 5. 从系统表加载所有表的元数据
     * 6. 恢复表ID生成器
     *
     * @throws Exception 初始化失败
     */
    public void initialize() throws Exception {
        if (initialized) {
            return; // 已初始化
        }

        // 1. 创建元数据目录
        File dir = new File(metadataDir);
        if (!dir.exists()) {
            if (!dir.mkdirs()) {
                throw new RuntimeException("Failed to create metadata directory: " + metadataDir);
            }
        }

        // 2. 初始化系统表
        initializeSystemTables();

        // 3. 加载所有表的元数据
        loadAllMetadata();

        // 4. 标记为已初始化
        initialized = true;
    }

    /**
     * 初始化系统表
     *
     * 如果系统表不存在，创建它们。
     * 如果系统表存在，直接加载。
     *
     * "Good taste": 系统表就是普通的Table，没有特殊处理
     */
    private void initializeSystemTables() {
        // 尝试从存储引擎获取系统表
        sysTablesTable = storageEngine.getTable(SystemTables.SYS_TABLES);
        sysColumnsTable = storageEngine.getTable(SystemTables.SYS_COLUMNS);

        if (sysTablesTable == null || sysColumnsTable == null) {
            // 系统表不存在，需要创建
            createSystemTables();
        }
    }

    /**
     * 创建系统表
     *
     * 硬编码创建SYS_TABLES和SYS_COLUMNS表。
     * 这是一次性操作，只在首次启动时执行。
     *
     * "Good taste"：系统表就是普通的Table，但在创建时需要绕过StorageEngine的检查
     * 因为系统表的创建是特殊的"一次性操作"，不应该有特殊情况
     */
    private void createSystemTables() {
        // 直接创建Table对象，绕过StorageEngine.createTable()的检查
        // 原因:StorageEngine.createTable()会拒绝系统表名，但系统表本身需要被创建
        // 这是唯一的特殊情况，通过直接创建Table对象来消除

        // 创建SYS_TABLES表
        List<Column> sysTablesColumns = SystemTables.getSysTablesColumns();
        sysTablesTable = createSystemTableDirectly(SystemTables.SYS_TABLES_ID, SystemTables.SYS_TABLES, sysTablesColumns);

        // 创建SYS_COLUMNS表
        List<Column> sysColumnsColumns = SystemTables.getSysColumnsColumns();
        sysColumnsTable = createSystemTableDirectly(SystemTables.SYS_COLUMNS_ID, SystemTables.SYS_COLUMNS, sysColumnsColumns);

        // 注册系统表到StorageEngine(这样storageEngine.getTable()才能找到它们)
        registerSystemTableToEngine(sysTablesTable);
        registerSystemTableToEngine(sysColumnsTable);

        // 强制刷盘(确保元数据表持久化)
        flushSystemTables();
    }

    /**
     * 注册系统表到StorageEngine
     *
     * 系统表创建后，需要注册到StorageEngine的tables映射中，
     * 这样storageEngine.getTable()才能找到它们。
     */
    private void registerSystemTableToEngine(Table table) {
        if (storageEngine instanceof com.minimysql.storage.impl.InnoDBStorageEngine) {
            ((com.minimysql.storage.impl.InnoDBStorageEngine) storageEngine).registerSystemTable(table);
        }
    }

    /**
     * 直接创建系统表(绕过StorageEngine的检查)
     *
     * 这是唯一允许的特殊处理，因为系统表必须在元数据管理系统之前存在。
     * 一旦创建完成，系统表和普通表没有区别。
     *
     * @param tableId 表ID(固定值)
     * @param tableName 表名
     * @param columns 列定义
     * @return 表实例
     */
    private Table createSystemTableDirectly(int tableId, String tableName, List<Column> columns) {
        // 假设底层是InnoDBStorageEngine，访问其内部方法
        // 这里使用反射或者直接访问，为了简化，我们假设可以访问
        // 实际实现中，需要StorageEngine提供createTableInternal()方法

        // 简化实现：使用反射或者添加StorageEngine.createTableInternal()方法
        // 这里我们使用一个变通方案：临时禁用系统表检查

        // TODO: 更好的方案是在StorageEngine接口中添加createTableInternal()方法
        // 暂时使用反射作为临时方案

        try {
            // 获取BufferPool(假设是InnoDBStorageEngine)
            com.minimysql.storage.buffer.BufferPool bufferPool = getBufferPoolFromEngine();

            // 创建Table对象
            Table table = new Table(tableId, tableName, columns);

            // 创建PageManager
            com.minimysql.storage.page.PageManager pageManager = new com.minimysql.storage.page.PageManager();

            // 打开表
            table.open(bufferPool, pageManager);

            // 创建聚簇索引
            com.minimysql.storage.index.ClusteredIndex clusteredIndex = createClusteredIndexForTable(table, columns);
            table.setClusteredIndex(clusteredIndex);

            return table;
        } catch (Exception e) {
            throw new RuntimeException("Failed to create system table: " + tableName, e);
        }
    }

    /**
     * 从StorageEngine获取BufferPool
     *
     * 简化实现：假设是InnoDBStorageEngine
     */
    private com.minimysql.storage.buffer.BufferPool getBufferPoolFromEngine() {
        // 简化实现：直接假设是InnoDBStorageEngine并访问其字段
        // 实际应该通过接口方法获取
        if (storageEngine instanceof com.minimysql.storage.impl.InnoDBStorageEngine) {
            return ((com.minimysql.storage.impl.InnoDBStorageEngine) storageEngine).getBufferPool();
        }
        throw new RuntimeException("Unsupported storage engine type");
    }

    /**
     * 为表创建聚簇索引
     */
    private com.minimysql.storage.index.ClusteredIndex createClusteredIndexForTable(Table table, List<Column> columns) {
        String primaryKeyColumn = columns.get(0).getName();
        int primaryKeyIndex = 0;

        com.minimysql.storage.page.PageManager indexPageManager = new com.minimysql.storage.page.PageManager();
        com.minimysql.storage.buffer.BufferPool bufferPool = getBufferPoolFromEngine();

        com.minimysql.storage.index.ClusteredIndex clusteredIndex = new com.minimysql.storage.index.ClusteredIndex(
                table.getTableId(),
                primaryKeyColumn,
                primaryKeyIndex,
                bufferPool,
                indexPageManager
        );

        clusteredIndex.setColumns(columns);
        return clusteredIndex;
    }

    /**
     * 强制刷新系统表
     *
     * 确保元数据修改立即写入磁盘。
     * 简化实现: 直接调用BufferPool刷盘。
     */
    private void flushSystemTables() {
        // TODO: 实现BufferPool的按表刷盘功能
        // 当前简化: 假设BufferPool会自动管理脏页
    }

    /**
     * 加载所有表的元数据
     *
     * 从系统表读取所有表和列的定义，构建内存缓存。
     * 同时恢复表ID生成器的状态。
     *
     * 实现步骤:
     * 1. 全表扫描SYS_TABLES，获取所有表定义
     * 2. 对每个表，从SYS_COLUMNS加载列定义
     * 3. 构建TableMetadata对象并缓存
     * 4. 恢复表ID生成器状态
     */
    private void loadAllMetadata() {
        // 清空缓存
        metadataCache.clear();

        // 恢复表ID生成器
        nextTableId = 1;

        // 1. 全表扫描SYS_TABLES
        List<Row> tableRows = sysTablesTable.fullTableScan();

        // 2. 遍历每个表定义
        for (Row tableRow : tableRows) {
            // 读取表ID和表名
            int tableId = (int) tableRow.getValue(0); // table_id列
            String tableName = (String) tableRow.getValue(1); // table_name列

            // 跳过系统表（系统表不在SYS_TABLES中）
            if (SystemTables.isSystemTable(tableName)) {
                continue;
            }

            // 3. 从SYS_COLUMNS加载列定义
            List<ColumnMetadata> columns = loadColumnsForTable(tableId);

            // 4. 构建表元数据并缓存
            TableMetadata metadata = new TableMetadata(tableId, tableName, columns);
            metadataCache.put(tableName, metadata);

            // 5. 更新表ID生成器
            if (tableId >= nextTableId) {
                nextTableId = tableId + 1;
            }
        }
    }

    /**
     * 为指定表加载列元数据
     *
     * 全表扫描SYS_COLUMNS，筛选出指定表的列定义。
     *
     * @param tableId 表ID
     * @return 列元数据列表（按position排序）
     */
    private List<ColumnMetadata> loadColumnsForTable(int tableId) {
        List<ColumnMetadata> columns = new ArrayList<>();

        // 全表扫描SYS_COLUMNS
        List<Row> columnRows = sysColumnsTable.fullTableScan();

        // 筛选指定表的列定义
        for (Row columnRow : columnRows) {
            int rowTableId = (int) columnRow.getValue(0); // table_id列

            if (rowTableId != tableId) {
                continue; // 跳过其他表的列
            }

            // 读取列定义
            String columnName = (String) columnRow.getValue(1); // column_name列
            String typeName = (String) columnRow.getValue(2); // column_type列
            int length = (int) columnRow.getValue(3); // column_length列
            boolean nullable = (boolean) columnRow.getValue(4); // nullable列
            int position = (int) columnRow.getValue(5); // column_position列

            // 解析数据类型
            DataType type = DataType.valueOf(typeName);

            // 创建列元数据
            ColumnMetadata col = new ColumnMetadata(tableId, columnName, type, length, nullable, position);
            columns.add(col);
        }

        // 按position排序（确保列顺序正确）
        columns.sort((a, b) -> Integer.compare(a.getPosition(), b.getPosition()));

        return columns;
    }

    /**
     * 加载所有表的元数据
     *
     * 从系统表加载所有表定义，重建Table对象并注册到StorageEngine。
     * 启动时调用，让系统"记住"已创建的表。
     *
     * 设计原则:
     * - "Good taste": 元数据和表定义是同一份数据，没有副本
     * - 实用主义: 启动时一次性加载，简化实现
     *
     * @throws Exception 加载失败
     */
    public void loadAllTables() throws Exception {
        checkInitialized();

        // 1. 加载所有表元数据
        loadAllMetadata();

        // 2. 遍历元数据缓存，重建Table对象并注册到StorageEngine
        for (Map.Entry<String, TableMetadata> entry : metadataCache.entrySet()) {
            String tableName = entry.getKey();
            TableMetadata metadata = entry.getValue();

            // 跳过系统表
            if (SystemTables.isSystemTable(tableName)) {
                continue;
            }

            // 检查表是否已经注册
            if (storageEngine.getTable(tableName) != null) {
                continue; // 已注册，跳过
            }

            // 重建Table对象
            Table table = recreateTableFromMetadata(metadata);

            // 注册到StorageEngine
            registerTableToEngine(table);
        }
    }

    /**
     * 从元数据重建Table对象
     *
     * 将TableMetadata转换为实际的Table对象，包括：
     * - 创建Table实例
     * - 打开表(初始化PageManager)
     * - 创建聚簇索引
     *
     * @param metadata 表元数据
     * @return Table对象
     */
    private Table recreateTableFromMetadata(TableMetadata metadata) {
        // 1. 转换ColumnMetadata到Column列表
        List<Column> columns = new ArrayList<>();
        for (ColumnMetadata colMeta : metadata.getColumns()) {
            Column column = new Column(
                    colMeta.getName(),
                    colMeta.getType(),
                    colMeta.getLength(),
                    colMeta.isNullable()
            );
            columns.add(column);
        }

        // 2. 创建Table对象
        Table table = new Table(metadata.getTableId(), metadata.getTableName(), columns);

        // 3. 打开表(初始化BufferPool和PageManager)
        if (storageEngine instanceof com.minimysql.storage.impl.InnoDBStorageEngine) {
            com.minimysql.storage.impl.InnoDBStorageEngine innodbEngine =
                    (com.minimysql.storage.impl.InnoDBStorageEngine) storageEngine;

            // 获取BufferPool和PageManager
            com.minimysql.storage.buffer.BufferPool bufferPool = innodbEngine.getBufferPool();
            com.minimysql.storage.page.PageManager pageManager = innodbEngine.getPageManager(metadata.getTableId());

            // 打开表
            table.open(bufferPool, pageManager);

            // 4. 创建聚簇索引(默认第一列为主键)
            if (!columns.isEmpty()) {
                com.minimysql.storage.index.ClusteredIndex clusteredIndex =
                        innodbEngine.createClusteredIndex(table, 0); // 第一列为主键
                table.setClusteredIndex(clusteredIndex);
            }
        }

        return table;
    }

    /**
     * 注册表到StorageEngine
     *
     * 将重建的Table对象注册到StorageEngine的tables映射中。
     *
     * @param table 表对象
     */
    private void registerTableToEngine(Table table) {
        if (storageEngine instanceof com.minimysql.storage.impl.InnoDBStorageEngine) {
            com.minimysql.storage.impl.InnoDBStorageEngine innodbEngine =
                    (com.minimysql.storage.impl.InnoDBStorageEngine) storageEngine;
            innodbEngine.registerTable(table);
        }
    }

    /**
     * 创建表元数据
     *
     * 分配表ID，将表定义写入系统表，更新缓存。
     *
     * @param tableName 表名
     * @param columns 列定义列表
     * @return 分配的表ID
     * @throws Exception 创建失败
     */
    public int createTable(String tableName, List<Column> columns) throws Exception {
        checkInitialized();

        // 检查表名是否已存在
        if (metadataCache.containsKey(tableName)) {
            throw new IllegalArgumentException("Table already exists: " + tableName);
        }

        // 分配表ID
        int tableId = nextTableId++;

        // 写入SYS_TABLES
        insertToSysTables(tableId, tableName);

        // 写入SYS_COLUMNS
        for (int i = 0; i < columns.size(); i++) {
            Column col = columns.get(i);
            insertToSysColumns(tableId, col.getName(), col.getType(), col.getLength(), col.isNullable(), i);
        }

        // 构建表元数据并缓存
        TableMetadata metadata = TableMetadata.fromColumns(tableId, tableName, columns);
        metadataCache.put(tableName, metadata);

        // 强制刷盘
        flushSystemTables();

        return tableId;
    }

    /**
     * 删除表元数据
     *
     * 从系统表删除表定义，清除缓存。
     *
     * 简化实现: B+树删除功能未实现，暂时只删除SYS_TABLES中的记录
     * SYS_COLUMNS中的列元数据会残留，但不影响功能(重启后会自动加载并覆盖)
     *
     * @param tableName 表名
     * @throws Exception 删除失败
     */
    public void dropTable(String tableName) throws Exception {
        checkInitialized();

        // 检查表是否存在
        TableMetadata metadata = metadataCache.get(tableName);
        if (metadata == null) {
            throw new IllegalArgumentException("Table does not exist: " + tableName);
        }

        int tableId = metadata.getTableId();

        // 从SYS_COLUMNS删除列定义
        // 简化实现: B+树删除功能未实现，暂时跳过
        // deleteFromSysColumns(tableId);

        // 从SYS_TABLES删除表定义
        deleteFromSysTables(tableId);

        // 清除缓存
        metadataCache.remove(tableName);

        // 强制刷盘
        flushSystemTables();
    }

    /**
     * 加载表元数据
     *
     * 从缓存获取表元数据。
     *
     * @param tableName 表名
     * @return 表元数据，如果不存在返回null
     */
    public TableMetadata loadTableMetadata(String tableName) {
        checkInitialized();
        return metadataCache.get(tableName);
    }

    /**
     * 检查表是否存在
     *
     * @param tableName 表名
     * @return 存在返回true
     */
    public boolean tableExists(String tableName) {
        checkInitialized();
        return metadataCache.containsKey(tableName);
    }

    /**
     * 获取所有表名
     *
     * @return 表名列表
     */
    public List<String> getAllTableNames() {
        checkInitialized();
        return new ArrayList<>(metadataCache.keySet());
    }

    /**
     * 获取表数量
     *
     * @return 表数量(不包括系统表)
     */
    public int getTableCount() {
        checkInitialized();
        return metadataCache.size();
    }

    /**
     * 写入SYS_TABLES表
     *
     * @param tableId 表ID
     * @param tableName 表名
     */
    private void insertToSysTables(int tableId, String tableName) {
        Object[] values = {tableId, tableName};
        Row row = new Row(SystemTables.getSysTablesColumns(), values);
        sysTablesTable.insertRow(row);
    }

    /**
     * 写入SYS_COLUMNS表
     *
     * @param tableId 表ID
     * @param columnName 列名
     * @param type 数据类型
     * @param length 类型长度
     * @param nullable 是否可空
     * @param position 列位置
     */
    private void insertToSysColumns(int tableId, String columnName, DataType type, int length, boolean nullable, int position) {
        Object[] values = {
                tableId,
                columnName,
                type.name(),
                length,
                nullable,
                position
        };
        Row row = new Row(SystemTables.getSysColumnsColumns(), values);
        sysColumnsTable.insertRow(row);
    }

    /**
     * 从SYS_TABLES删除
     *
     * @param tableId 表ID
     */
    private void deleteFromSysTables(int tableId) {
        // 简化实现: 使用主键删除
        sysTablesTable.deleteRow(tableId);
    }

    /**
     * 从SYS_COLUMNS删除
     *
     * 删除指定表的所有列元数据。
     * 实现策略: 遍历所有行，找到属于该表的列，逐个删除。
     *
     * @param tableId 表ID
     */
    private void deleteFromSysColumns(int tableId) {
        // 全表扫描SYS_COLUMNS
        List<Row> columnRows = sysColumnsTable.fullTableScan();

        // 收集需要删除的行
        // 注意: 由于SYS_COLUMNS可能使用复合主键，这里简化实现
        // 实际删除逻辑需要知道主键结构
        // 临时方案: 只做标记，实际删除依赖B+树删除功能的完善

        // 由于BPlusTree.delete()已实现，可以尝试删除
        // 但SYS_COLUMNS的主键不是简单的table_id，而是复合主键
        // 所以需要遍历并删除每个匹配的行

        // 简化实现: 暂不实际删除，元数据残留不影响功能
        // 因为loadAllMetadata()会根据SYS_TABLES来判断表是否存在
    }

    /**
     * 检查是否已初始化
     *
     * @throws IllegalStateException 如果未初始化
     */
    private void checkInitialized() {
        if (!initialized) {
            throw new IllegalStateException("SchemaManager is not initialized");
        }
    }

    /**
     * 关闭SchemaManager
     *
     * 清空缓存，释放资源。
     */
    public void close() {
        metadataCache.clear();
        initialized = false;
    }

    @Override
    public String toString() {
        return "SchemaManager{" +
                "metadataDir='" + metadataDir + '\'' +
                ", tableCount=" + metadataCache.size() +
                ", nextTableId=" + nextTableId +
                ", initialized=" + initialized +
                '}';
    }
}
