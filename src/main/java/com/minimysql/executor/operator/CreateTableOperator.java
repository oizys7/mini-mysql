package com.minimysql.executor.operator;

import com.minimysql.executor.Operator;
import com.minimysql.storage.StorageEngine;
import com.minimysql.storage.table.Column;
import com.minimysql.storage.table.Row;
import com.minimysql.storage.table.Table;

import java.util.List;

/**
 * CreateTableOperator - CREATE TABLE算子
 *
 * 负责执行CREATE TABLE语句,创建新表。
 *
 * 核心功能:
 * 1. 调用StorageEngine创建表
 * 2. 返回创建的表对象
 *
 * 设计原则:
 * - "Good taste": 简单直接,委托给StorageEngine
 * - 不支持迭代: hasNext()始终返回false
 * - 执行后返回: execute()返回创建的Table对象
 *
 * 使用示例:
 * <pre>
 * CreateTableOperator create = new CreateTableOperator(
 *     storageEngine,
 *     "users",
 *     columns
 * );
 *
 * Table table = create.execute();
 * </pre>
 *
 * 设计哲学:
 * - "Theory and practice sometimes clash. Theory loses. Every single time."
 * - 不实现复杂的表结构验证,让StorageEngine处理
 */
public class CreateTableOperator implements Operator {

    /** 存储引擎 */
    private final StorageEngine storageEngine;

    /** 表名 */
    private final String tableName;

    /** 列定义列表 */
    private final List<Column> columns;

    /** 是否已执行 */
    private boolean executed = false;

    /** 创建的表对象 */
    private Table createdTable;

    /**
     * 创建CREATE TABLE算子
     *
     * @param storageEngine 存储引擎
     * @param tableName 表名
     * @param columns 列定义列表
     */
    public CreateTableOperator(StorageEngine storageEngine, String tableName, List<Column> columns) {
        if (storageEngine == null) {
            throw new IllegalArgumentException("StorageEngine cannot be null");
        }
        if (tableName == null || tableName.trim().isEmpty()) {
            throw new IllegalArgumentException("Table name cannot be null or empty");
        }
        if (columns == null || columns.isEmpty()) {
            throw new IllegalArgumentException("Columns cannot be null or empty");
        }

        this.storageEngine = storageEngine;
        this.tableName = tableName;
        this.columns = columns;
    }

    /**
     * 检查是否还有下一行
     *
     * CREATE TABLE算子不支持迭代模式,直接返回false。
     * 调用方应该使用execute()方法执行创建。
     *
     * @return 始终返回false
     */
    @Override
    public boolean hasNext() {
        return false;
    }

    /**
     * 获取下一行
     *
     * CREATE TABLE算子不支持迭代模式,直接抛出异常。
     * 调用方应该使用execute()方法执行创建。
     *
     * @return 始终抛出异常
     */
    @Override
    public Row next() {
        throw new UnsupportedOperationException(
                "CreateTableOperator does not support iteration. Use execute() instead."
        );
    }

    /**
     * 执行CREATE TABLE操作
     *
     * 调用StorageEngine.createTable()创建表。
     *
     * @return 创建的Table对象
     */
    public Table execute() {
        if (executed) {
            throw new IllegalStateException("CreateTableOperator can only be executed once");
        }

        executed = true;

        // 调用StorageEngine创建表
        createdTable = storageEngine.createTable(tableName, columns);

        return createdTable;
    }

    /**
     * 获取创建的表对象
     *
     * 必须在execute()之后调用。
     *
     * @return 创建的表对象
     */
    public Table getCreatedTable() {
        if (!executed) {
            throw new IllegalStateException("CreateTableOperator has not been executed yet");
        }
        return createdTable;
    }

    /**
     * 获取表名
     *
     * @return 表名
     */
    public String getTableName() {
        return tableName;
    }

    /**
     * 获取列定义列表
     *
     * @return 列定义列表
     */
    public List<Column> getColumns() {
        return columns;
    }

    @Override
    public String toString() {
        return "CreateTableOperator{" +
                "tableName='" + tableName + '\'' +
                ", columns=" + columns.size() +
                '}';
    }
}
