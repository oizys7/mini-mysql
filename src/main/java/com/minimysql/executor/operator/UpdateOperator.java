package com.minimysql.executor.operator;

import com.minimysql.executor.ExpressionEvaluator;
import com.minimysql.executor.Operator;
import com.minimysql.parser.Expression;
import com.minimysql.storage.table.Column;
import com.minimysql.storage.table.DataType;
import com.minimysql.storage.table.Row;
import com.minimysql.storage.table.Table;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * UpdateOperator - UPDATE更新算子
 *
 * 负责执行UPDATE语句,更新表中符合WHERE条件的数据行。
 *
 * 核心功能:
 * 1. 全表扫描: 扫描表中的所有行
 * 2. WHERE条件过滤: 使用ExpressionEvaluator评估WHERE条件
 * 3. 值更新: 对符合条件的行,更新指定列的值
 * 4. 表达式求值: 将SET子句中的表达式转换为实际值
 * 5. 返回影响行数
 *
 * 设计原则:
 * - "Good taste": 简单直接,先扫描再过滤再更新
 * - 实用主义: 不实现复杂的索引查找优化,直接全表扫描
 * - 简单直接: 不实现ORDER BY、LIMIT等子句
 *
 * MySQL对应:
 * - MySQL的UPDATE语句执行(无索引优化情况)
 * - 对应EXPLAIN输出中的type=ALL
 *
 * 使用示例:
 * <pre>
 * // UPDATE users SET age = 26 WHERE id = 1
 * Map<String, Expression> assignments = Map.of("age", new LiteralExpression(26));
 * Expression whereClause = new BinaryExpression(
 *     new ColumnExpression("id"),
 *     Operator.EQUAL,
 *     new LiteralExpression(1)
 * );
 *
 * UpdateOperator updateOp = new UpdateOperator(table, assignments, whereClause, evaluator);
 * int affectedRows = updateOp.execute();
 * </pre>
 *
 * 数据流:
 * Table.fullTableScan() → WHERE过滤 → 更新符合条件的行 → Table.updateRow() → 影响行数
 *
 * 性能特点:
 * - O(N)时间复杂度,N为表中的总行数
 * - 需要全表扫描(即使只更新一行)
 * - 不使用索引优化(简化实现)
 *
 * 设计哲学:
 * - "Bad programmers worry about the code. Good programmers worry about data structures."
 * - 全表扫描虽然理论上慢,但在小表或无索引场景是最实用的方案
 * - 不实现复杂的索引查找逻辑,保持简单
 *
 * 注意事项:
 * - 没有WHERE子句会更新所有行!
 * - 主键列不允许更新
 * - 不支持事务,更新失败不会回滚
 */
public class UpdateOperator implements Operator {

    /** 表对象 */
    private final Table table;

    /** 更新映射(列名 -> 新值表达式) */
    private final Map<String, Expression> assignments;

    /** WHERE条件(为空表示更新所有行) */
    private final Expression whereClause;

    /** 表达式求值器 */
    private final ExpressionEvaluator evaluator;

    /** 是否已执行 */
    private boolean executed = false;

    /** 受影响的行数 */
    private int affectedRows = 0;

    /** 更新结果行(用于next()返回,可选) */
    private List<Row> updatedRows;
    private int currentRowIndex = 0;

    /**
     * 创建UPDATE算子
     *
     * @param table 表对象
     * @param assignments 更新映射(列名 -> 新值表达式)
     * @param whereClause WHERE条件(为null表示更新所有行)
     * @param evaluator 表达式求值器
     */
    public UpdateOperator(Table table,
                          Map<String, Expression> assignments,
                          Expression whereClause,
                          ExpressionEvaluator evaluator) {
        if (table == null) {
            throw new IllegalArgumentException("Table cannot be null");
        }
        if (assignments == null || assignments.isEmpty()) {
            throw new IllegalArgumentException("Assignments cannot be null or empty");
        }
        if (evaluator == null) {
            throw new IllegalArgumentException("ExpressionEvaluator cannot be null");
        }

        this.table = table;
        this.assignments = assignments;
        this.whereClause = whereClause;
        this.evaluator = evaluator;
        this.updatedRows = new ArrayList<>();
    }

    /**
     * 检查是否还有下一行
     *
     * 如果执行了execute()并且有更新行,返回true。
     *
     * @return 如果还有更新行返回true
     */
    @Override
    public boolean hasNext() {
        return currentRowIndex < updatedRows.size();
    }

    /**
     * 获取下一个更新后的行
     *
     * 必须先调用execute()。
     *
     * @return 更新后的行数据
     */
    @Override
    public Row next() {
        if (!hasNext()) {
            throw new java.util.NoSuchElementException("No more rows");
        }
        return updatedRows.get(currentRowIndex++);
    }

    /**
     * 执行UPDATE操作
     *
     * 算法流程:
     * 1. 全表扫描所有行
     * 2. 对每一行:
     *    a) 评估WHERE条件(如果有)
     *    b) 如果符合条件,更新指定的列
     *    c) 调用Table.updateRow()保存更改
     * 3. 返回影响行数
     *
     * @return 受影响的行数
     */
    public int execute() {
        if (executed) {
            throw new IllegalStateException("UpdateOperator can only be executed once");
        }

        executed = true;
        affectedRows = 0;

        // 获取表的列定义
        List<Column> tableColumns = table.getColumns();

        // 获取聚簇索引(用于按主键查找和更新)
        if (table.getClusteredIndex() == null) {
            throw new IllegalStateException("Table does not have a clustered index");
        }

        // 找到主键列
        int primaryKeyIndex = table.getClusteredIndex().getPrimaryKeyIndex();
        Column primaryKeyColumn = tableColumns.get(primaryKeyIndex);

        // 检查是否尝试更新主键列
        for (String columnName : assignments.keySet()) {
            if (columnName.equalsIgnoreCase(primaryKeyColumn.getName())) {
                throw new IllegalArgumentException(
                        "Cannot update primary key column: " + columnName
                );
            }
        }

        // 构建更新列的索引映射
        Map<Integer, Expression> updateIndexes = buildUpdateIndexes(tableColumns, assignments);

        // 全表扫描
        List<Row> allRows = table.fullTableScan();

        // 逐行处理
        for (Row row : allRows) {
            try {
                // 1. 评估WHERE条件
                if (whereClause != null) {
                    Boolean conditionResult = (Boolean) evaluator.eval(whereClause, row, tableColumns);
                    if (conditionResult == null || !conditionResult) {
                        continue; // 不符合条件,跳过
                    }
                }

                // 2. 更新列值
                Row updatedRow = createUpdatedRow(row, updateIndexes, tableColumns);

                // 3. 获取主键值
                Object primaryKeyValue = row.getValue(primaryKeyIndex);

                // 4. 调用Table.updateRow()更新
                int updateResult = table.updateRow(primaryKeyValue, updatedRow);

                if (updateResult > 0) {
                    affectedRows++;
                    updatedRows.add(updatedRow);
                }

            } catch (IllegalArgumentException e) {
                // 参数校验异常,直接抛出
                throw e;
            } catch (Exception e) {
                // 其他异常包装为RuntimeException
                throw new RuntimeException("Failed to update row: " + e.getMessage(), e);
            }
        }

        return affectedRows;
    }

    /**
     * 构建更新列的索引映射
     *
     * 将列名映射到列在表定义中的索引。
     *
     * @param tableColumns 表的列定义
     * @param assignments 更新映射(列名 -> 表达式)
     * @return 列索引 -> 表达式的映射
     */
    private Map<Integer, Expression> buildUpdateIndexes(List<Column> tableColumns,
                                                         Map<String, Expression> assignments) {
        Map<Integer, Expression> result = new java.util.HashMap<>();

        for (Map.Entry<String, Expression> entry : assignments.entrySet()) {
            String columnName = entry.getKey();
            int index = -1;

            // 查找列在表定义中的索引
            for (int i = 0; i < tableColumns.size(); i++) {
                if (tableColumns.get(i).getName().equalsIgnoreCase(columnName)) {
                    index = i;
                    break;
                }
            }

            if (index == -1) {
                throw new IllegalArgumentException("Column not found: " + columnName);
            }

            result.put(index, entry.getValue());
        }

        return result;
    }

    /**
     * 创建更新后的Row对象
     *
     * 复制原Row,然后更新指定列的值。
     *
     * @param originalRow 原始行
     * @param updateIndexes 更新列索引映射
     * @param tableColumns 表的列定义
     * @return 更新后的行
     */
    private Row createUpdatedRow(Row originalRow,
                                  Map<Integer, Expression> updateIndexes,
                                  List<Column> tableColumns) {
        // 复制原始值
        Object[] newValues = new Object[tableColumns.size()];
        for (int i = 0; i < tableColumns.size(); i++) {
            newValues[i] = originalRow.getValue(i);
        }

        // 更新指定列
        for (Map.Entry<Integer, Expression> entry : updateIndexes.entrySet()) {
            int columnIndex = entry.getKey();
            Expression expression = entry.getValue();

            // 表达式求值
            Object newValue = evaluator.eval(expression, originalRow, tableColumns);

            // 类型转换
            Column column = tableColumns.get(columnIndex);
            newValue = convertValue(newValue, column);

            newValues[columnIndex] = newValue;
        }

        // 创建新的Row对象
        return new Row(tableColumns, newValues);
    }

    /**
     * 类型转换
     *
     * 确保值类型与列定义匹配,如果不匹配则尝试转换。
     *
     * @param value 原始值
     * @param column 列定义
     * @return 转换后的值
     */
    private Object convertValue(Object value, Column column) {
        // NULL值处理
        if (value == null) {
            if (!column.isNullable()) {
                throw new IllegalArgumentException(
                        "Column '" + column.getName() + "' cannot be null"
                );
            }
            return null;
        }

        DataType targetType = column.getType();

        // 如果类型已经匹配,直接返回
        if (isTypeMatch(value, targetType)) {
            return value;
        }

        // 类型转换
        try {
            switch (targetType) {
                case INT:
                    if (value instanceof Number) {
                        return ((Number) value).intValue();
                    } else if (value instanceof String) {
                        return Integer.parseInt((String) value);
                    }
                    break;

                case VARCHAR:
                    return value.toString();

                default:
                    throw new IllegalArgumentException(
                            "Unsupported type conversion: " + value.getClass() + " -> " + targetType
                    );
            }
        } catch (Exception e) {
            throw new IllegalArgumentException(
                    "Failed to convert value for column '" + column.getName() + "': " + e.getMessage(),
                    e
            );
        }

        throw new IllegalArgumentException(
                "Type mismatch for column '" + column.getName() + "': " +
                        "expected " + targetType + ", got " + value.getClass().getSimpleName()
        );
    }

    /**
     * 检查值类型是否匹配列定义
     *
     * @param value 值
     * @param targetType 目标类型
     * @return 如果匹配返回true
     */
    private boolean isTypeMatch(Object value, DataType targetType) {
        if (value == null) {
            return true;
        }

        switch (targetType) {
            case INT:
                return value instanceof Integer;
            case VARCHAR:
                return value instanceof String;
            default:
                return false;
        }
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
            throw new IllegalStateException("UpdateOperator has not been executed yet");
        }
        return affectedRows;
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
        return "UpdateOperator{" +
                "table=" + table.getTableName() +
                ", assignments=" + assignments.keySet() +
                ", whereClause=" + (whereClause != null ? "present" : "absent") +
                '}';
    }
}
