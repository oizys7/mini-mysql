package com.minimysql.executor.operator;

import com.minimysql.executor.Operator;
import com.minimysql.storage.StorageEngine;
import com.minimysql.storage.table.Row;
import com.minimysql.storage.table.Table;

/**
 * DropTableOperator - DROP TABLE算子
 *
 * 负责执行DROP TABLE语句,删除表。
 *
 * 核心功能:
 * 1. 调用StorageEngine删除表
 * 2. 返回是否成功
 *
 * 设计原则:
 * - "Good taste": 简单直接,委托给StorageEngine
 * - 不支持迭代: hasNext()始终返回false
 * - 执行后返回: execute()返回是否成功
 *
 * 使用示例:
 * <pre>
 * DropTableOperator drop = new DropTableOperator(
 *     storageEngine,
 *     "users"
 * );
 *
 * boolean success = drop.execute();
 * </pre>
 *
 * 设计哲学:
 * - "Theory and practice sometimes clash. Theory loses. Every single time."
 * - 不实现复杂的依赖检查,让StorageEngine处理
 */
public class DropTableOperator implements Operator {

    /** 存储引擎 */
    private final StorageEngine storageEngine;

    /** 表名 */
    private final String tableName;

    /** 是否已执行 */
    private boolean executed = false;

    /** 是否成功 */
    private boolean success;

    /**
     * 创建DROP TABLE算子
     *
     * @param storageEngine 存储引擎
     * @param tableName 表名
     */
    public DropTableOperator(StorageEngine storageEngine, String tableName) {
        if (storageEngine == null) {
            throw new IllegalArgumentException("StorageEngine cannot be null");
        }
        if (tableName == null || tableName.trim().isEmpty()) {
            throw new IllegalArgumentException("Table name cannot be null or empty");
        }

        this.storageEngine = storageEngine;
        this.tableName = tableName;
    }

    /**
     * 检查是否还有下一行
     *
     * DROP TABLE算子不支持迭代模式,直接返回false。
     * 调用方应该使用execute()方法执行删除。
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
     * DROP TABLE算子不支持迭代模式,直接抛出异常。
     * 调用方应该使用execute()方法执行删除。
     *
     * @return 始终抛出异常
     */
    @Override
    public Row next() {
        throw new UnsupportedOperationException(
                "DropTableOperator does not support iteration. Use execute() instead."
        );
    }

    /**
     * 执行DROP TABLE操作
     *
     * 调用StorageEngine.dropTable()删除表。
     *
     * @return 是否成功
     */
    public boolean execute() {
        if (executed) {
            throw new IllegalStateException("DropTableOperator can only be executed once");
        }

        executed = true;

        try {
            // 调用StorageEngine删除表
            storageEngine.dropTable(tableName);
            success = true;
        } catch (Exception e) {
            success = false;
            throw e;
        }

        return success;
    }

    /**
     * 检查是否执行成功
     *
     * 必须在execute()之后调用。
     *
     * @return 是否成功
     */
    public boolean isSuccess() {
        if (!executed) {
            throw new IllegalStateException("DropTableOperator has not been executed yet");
        }
        return success;
    }

    /**
     * 获取表名
     *
     * @return 表名
     */
    public String getTableName() {
        return tableName;
    }

    @Override
    public String toString() {
        return "DropTableOperator{" +
                "tableName='" + tableName + '\'' +
                '}';
    }
}
