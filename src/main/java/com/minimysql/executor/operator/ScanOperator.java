package com.minimysql.executor.operator;

import com.minimysql.executor.Operator;
import com.minimysql.storage.table.Row;
import com.minimysql.storage.table.Table;

import java.util.Iterator;
import java.util.List;

/**
 * ScanOperator - 全表扫描算子
 *
 * 最基础的算子,负责从表中读取所有行数据。
 * 作为算子树的叶子节点,为上层算子提供数据源。
 *
 * 设计原则:
 * - "Good taste": 简单直接,没有特殊情况
 * - 懒加载: 调用fullTableScan()后立即获得Iterator,逐行返回
 * - 无状态: 不持有任何中间状态,每次调用hasNext()/next()都委托给Iterator
 *
 * MySQL对应:
 * - MySQL中的全表扫描(扫描聚簇索引的所有叶子节点)
 * - 对应EXPLAIN输出中的type=ALL
 *
 * 使用示例:
 * <pre>
 * Operator scan = new ScanOperator(table);
 * while (scan.hasNext()) {
 *     Row row = scan.next();
 *     // 处理行数据
 * }
 * </pre>
 *
 * 数据流:
 * Table.fullTableScan() → List<Row> → Iterator → ScanOperator.hasNext()/next()
 *
 * 性能特点:
 * - O(N)时间复杂度,需要读取所有行
 * - 内存友好:使用Iterator,不会一次性加载所有数据到内存
 *
 * 设计哲学:
 * - "Theory and practice sometimes clash. Theory loses. Every single time."
 * - 全表扫描虽然理论上慢,但在小表或无索引场景是最实用的方案
 * - 不实现复杂的索引选择逻辑,保持简单
 */
public class ScanOperator implements Operator {

    /** 表对象 */
    private final Table table;

    /** 行数据迭代器 */
    private final Iterator<Row> rowIterator;

    /** 当前行(用于next()返回) */
    private Row currentRow;

    /**
     * 创建全表扫描算子
     *
     * @param table 要扫描的表
     */
    public ScanOperator(Table table) {
        if (table == null) {
            throw new IllegalArgumentException("Table cannot be null");
        }

        this.table = table;

        // 立即执行全表扫描,获得迭代器
        // 设计决策:不在构造函数中缓存所有行,而是直接获得Iterator
        // 原因:
        // 1. 懒加载:调用方不需要等待全表扫描完成
        // 2. 内存友好: Iterator模式,按需读取
        // 3. 简单直接:不实现复杂的异步加载逻辑

        List<Row> allRows = table.fullTableScan();
        this.rowIterator = allRows.iterator();
    }

    /**
     * 检查是否还有下一行
     *
     * @return 如果还有下一行返回true
     */
    @Override
    public boolean hasNext() {
        return rowIterator.hasNext();
    }

    /**
     * 获取下一行数据
     *
     * @return 行数据
     * @throws java.util.NoSuchElementException 如果没有下一行
     */
    @Override
    public Row next() {
        if (!hasNext()) {
            throw new java.util.NoSuchElementException("No more rows");
        }

        currentRow = rowIterator.next();
        return currentRow;
    }

    /**
     * 获取表对象
     *
     * @return 表对象
     */
    public Table getTable() {
        return table;
    }

    @Override
    public String toString() {
        return "ScanOperator{" +
                "table=" + table.getTableName() +
                '}';
    }
}
