package com.minimysql.executor.operator;

import com.minimysql.executor.ExpressionEvaluator;
import com.minimysql.executor.Operator;
import com.minimysql.parser.Expression;
import com.minimysql.storage.table.Column;
import com.minimysql.storage.table.Row;
import com.minimysql.storage.table.Table;

import java.util.ArrayList;
import java.util.List;

/**
 * DeleteOperator - DELETE删除算子
 *
 * 负责执行DELETE语句,删除表中符合WHERE条件的数据行。
 *
 * 核心功能:
 * 1. 全表扫描: 扫描表中的所有行
 * 2. WHERE条件过滤: 使用ExpressionEvaluator评估WHERE条件
 * 3. 行删除: 对符合条件的行,调用Table.deleteRow()删除
 * 4. 返回影响行数
 *
 * 设计原则:
 * - "Good taste": 简单直接,先扫描再过滤再删除
 * - 实用主义: 不实现复杂的索引查找优化,直接全表扫描
 * - 简单直接: 不实现ORDER BY、LIMIT等子句
 *
 * MySQL对应:
 * - MySQL的DELETE语句执行(无索引优化情况)
 * - 对应EXPLAIN输出中的type=ALL
 *
 * 使用示例:
 * <pre>
 * // DELETE FROM users WHERE id = 1
 * Expression whereClause = new BinaryExpression(
 *     new ColumnExpression("id"),
 *     Operator.EQUAL,
 *     new LiteralExpression(1)
 * );
 *
 * DeleteOperator deleteOp = new DeleteOperator(table, whereClause, evaluator);
 * int affectedRows = deleteOp.execute();
 * </pre>
 *
 * 数据流:
 * Table.fullTableScan() → WHERE过滤 → 收集主键 → Table.deleteRow() → 影响行数
 *
 * 性能特点:
 * - O(N)时间复杂度,N为表中的总行数
 * - 需要全表扫描(即使只删除一行)
 * - 不使用索引优化(简化实现)
 *
 * 设计哲学:
 * - "Bad programmers worry about the code. Good programmers worry about data structures."
 * - 全表扫描虽然理论上慢,但在小表或无索引场景是最实用的方案
 * - 不实现复杂的索引查找逻辑,保持简单
 *
 * 注意事项:
 * - 没有WHERE子句会删除所有行!
 * - 不支持事务,删除失败不会回滚
 * - 删除操作不可逆(除非有备份)
 *
 * 已知限制:
 * - B+树删除功能不完整,二级索引删除可能失败
 * - 删除后不会回收空间(页不会合并)
 */
public class DeleteOperator implements Operator {

    /** 表对象 */
    private final Table table;

    /** WHERE条件(为空表示删除所有行) */
    private final Expression whereClause;

    /** 表达式求值器 */
    private final ExpressionEvaluator evaluator;

    /** 是否已执行 */
    private boolean executed = false;

    /** 受影响的行数 */
    private int affectedRows = 0;

    /** 删除的主键列表(用于调试) */
    private List<Object> deletedPrimaryKeys;

    /**
     * 创建DELETE算子
     *
     * @param table 表对象
     * @param whereClause WHERE条件(为null表示删除所有行)
     * @param evaluator 表达式求值器
     */
    public DeleteOperator(Table table,
                          Expression whereClause,
                          ExpressionEvaluator evaluator) {
        if (table == null) {
            throw new IllegalArgumentException("Table cannot be null");
        }
        if (evaluator == null) {
            throw new IllegalArgumentException("ExpressionEvaluator cannot be null");
        }

        this.table = table;
        this.whereClause = whereClause;
        this.evaluator = evaluator;
        this.deletedPrimaryKeys = new ArrayList<>();
    }

    /**
     * 检查是否还有下一行
     *
     * DELETE算子不支持迭代模式,直接返回false。
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
     * DELETE算子不支持迭代模式,直接抛出异常。
     * 调用方应该使用execute()方法执行删除。
     *
     * @return 始终抛出异常
     */
    @Override
    public Row next() {
        throw new UnsupportedOperationException(
                "DeleteOperator does not support iteration. Use execute() instead."
        );
    }

    /**
     * 执行DELETE操作
     *
     * 算法流程:
     * 1. 全表扫描所有行
     * 2. 对每一行:
     *    a) 评估WHERE条件(如果有)
     *    b) 如果符合条件,记录主键值
     * 3. 对所有符合条件的主键,调用Table.deleteRow()删除
     * 4. 返回影响行数
     *
     * 注意: 先收集所有要删除的主键,再执行删除,避免在迭代过程中修改表结构
     *
     * @return 受影响的行数
     */
    public int execute() {
        if (executed) {
            throw new IllegalStateException("DeleteOperator can only be executed once");
        }

        executed = true;
        affectedRows = 0;

        // 获取聚簇索引(用于按主键删除)
        if (table.getClusteredIndex() == null) {
            throw new IllegalStateException("Table does not have a clustered index");
        }

        // 找到主键列
        int primaryKeyIndex = table.getClusteredIndex().getPrimaryKeyIndex();
        List<Column> tableColumns = table.getColumns();

        // 全表扫描
        List<Row> allRows = table.fullTableScan();

        // 第一遍:收集要删除的主键
        List<Object> primaryKeysToDelete = new ArrayList<>();

        for (Row row : allRows) {
            try {
                // 评估WHERE条件
                if (whereClause != null) {
                    Boolean conditionResult = (Boolean) evaluator.eval(whereClause, row, tableColumns);
                    if (conditionResult == null || !conditionResult) {
                        continue; // 不符合条件,跳过
                    }
                }

                // 符合条件,记录主键
                Object primaryKeyValue = row.getValue(primaryKeyIndex);
                primaryKeysToDelete.add(primaryKeyValue);

            } catch (Exception e) {
                // 简化实现:遇到错误直接抛出,不实现部分成功回滚
                throw new RuntimeException("Failed to evaluate WHERE condition: " + e.getMessage(), e);
            }
        }

        // 第二遍:执行删除
        for (Object primaryKeyValue : primaryKeysToDelete) {
            try {
                int deleteResult = table.deleteRow(primaryKeyValue);

                if (deleteResult > 0) {
                    affectedRows++;
                    deletedPrimaryKeys.add(primaryKeyValue);
                }

            } catch (Exception e) {
                // 简化实现:遇到错误直接抛出,不实现部分成功回滚
                throw new RuntimeException(
                        "Failed to delete row with primary key " + primaryKeyValue + ": " + e.getMessage(),
                        e
                );
            }
        }

        return affectedRows;
    }

    /**
     * 获取受影响的行数
     *
     * 必须在execute()之后调用。
     *
     * @return 受影响的行数
     */
    public int getAffectedRows() {
        if (!executed) {
            throw new IllegalStateException("DeleteOperator has not been executed yet");
        }
        return affectedRows;
    }

    /**
     * 获取删除的主键列表
     *
     * 必须在execute()之后调用。
     * 主要用于调试和测试。
     *
     * @return 删除的主键列表
     */
    public List<Object> getDeletedPrimaryKeys() {
        if (!executed) {
            throw new IllegalStateException("DeleteOperator has not been executed yet");
        }
        return new ArrayList<>(deletedPrimaryKeys);
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
        return "DeleteOperator{" +
                "table=" + table.getTableName() +
                ", whereClause=" + (whereClause != null ? "present" : "absent") +
                '}';
    }
}
