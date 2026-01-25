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

/**
 * InsertOperator - INSERT插入算子
 *
 * 负责执行INSERT语句,将一行或多行数据插入到表中。
 *
 * 核心功能:
 * 1. 表达式求值: 将INSERT中的表达式(字面量)转换为实际值
 * 2. 列顺序映射: 如果指定了列名,按指定顺序插入;否则按表定义顺序
 * 3. 类型检查: 确保插入值的类型与列定义匹配
 * 4. 批量插入: 支持一次插入多行
 * 5. 返回影响行数
 *
 * 设计原则:
 * - "Good taste": 没有特殊情况,单行和批量插入统一处理
 * - 实用主义: 直接调用Table.insertRow(),不实现复杂的批量优化
 * - 简单直接: 不实现ON DUPLICATE KEY UPDATE等复杂语法
 *
 * MySQL对应:
 * - MySQL的INSERT语句执行
 * - 对应INSERT的执行阶段(Optimizer之后)
 *
 * 使用示例:
 * <pre>
 * // INSERT INTO users VALUES (1, 'Alice', 25)
 * List<String> columns = List.of();
 * List<List<Expression>> values = List.of(
 *     List.of(new LiteralExpression(1),
 *             new LiteralExpression("Alice"),
 *             new LiteralExpression(25))
 * );
 *
 * InsertOperator insertOp = new InsertOperator(table, columns, values, evaluator);
 * int affectedRows = insertOp.execute();
 * </pre>
 *
 * 数据流:
 * InsertStatement → 表达式求值 → 类型检查 → Table.insertRow() → 影响行数
 *
 * 性能特点:
 * - O(N)时间复杂度,N为插入行数
 * - 每行独立插入,不使用批处理优化
 *
 * 设计哲学:
 * - "Theory and practice sometimes clash. Theory loses. Every single time."
 * - 不实现复杂的批量插入优化(如LOAD DATA INFILE),保持简单
 * - 类型检查宽松:只要能转换就允许,不强制完全匹配
 */
public class InsertOperator implements Operator {

    /** 表对象 */
    private final Table table;

    /** 列名列表(为空表示按表定义顺序) */
    private final List<String> columnNames;

    /** 待插入的值列表(每个元素代表一行,每行是一个表达式列表) */
    private final List<List<Expression>> values;

    /** 表达式求值器 */
    private final ExpressionEvaluator evaluator;

    /** 是否已执行 */
    private boolean executed = false;

    /** 受影响的行数 */
    private int affectedRows = 0;

    /**
     * 创建INSERT算子
     *
     * @param table 表对象
     * @param columnNames 列名列表(为空表示按表定义顺序)
     * @param values 待插入的值列表
     * @param evaluator 表达式求值器
     */
    public InsertOperator(Table table,
                          List<String> columnNames,
                          List<List<Expression>> values,
                          ExpressionEvaluator evaluator) {
        if (table == null) {
            throw new IllegalArgumentException("Table cannot be null");
        }
        if (values == null || values.isEmpty()) {
            throw new IllegalArgumentException("Values cannot be null or empty");
        }
        if (evaluator == null) {
            throw new IllegalArgumentException("ExpressionEvaluator cannot be null");
        }

        this.table = table;
        this.columnNames = columnNames != null ? columnNames : List.of();
        this.values = values;
        this.evaluator = evaluator;
    }

    /**
     * 检查是否还有下一行
     *
     * INSERT算子不支持迭代模式,直接返回false。
     * 调用方应该使用execute()方法执行插入。
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
     * INSERT算子不支持迭代模式,直接抛出异常。
     * 调用方应该使用execute()方法执行插入。
     *
     * @return 始终抛出异常
     */
    @Override
    public Row next() {
        throw new UnsupportedOperationException(
                "InsertOperator does not support iteration. Use execute() instead."
        );
    }

    /**
     * 执行INSERT操作
     *
     * 对每一行数据:
     * 1. 表达式求值(将Expression转换为实际值)
     * 2. 列顺序映射(如果指定了列名)
     * 3. 类型检查和转换
     * 4. 调用Table.insertRow()插入
     *
     * @return 受影响的行数
     */
    public int execute() {
        if (executed) {
            throw new IllegalStateException("InsertOperator can only be executed once");
        }

        executed = true;
        affectedRows = 0;

        // 获取表的列定义
        List<Column> tableColumns = table.getColumns();

        // 构建列索引映射(如果指定了列名)
        int[] columnIndexes = buildColumnIndexes(tableColumns, columnNames);

        // 逐行插入
        for (List<Expression> rowValues : values) {
            try {
                // 1. 表达式求值
                Object[] values = evaluateExpressions(rowValues);

                // 2. 列顺序映射和类型转换
                Object[] mappedValues = mapAndConvertValues(tableColumns, columnIndexes, values);

                // 3. 创建Row对象
                Row row = new Row(mappedValues);

                // 4. 插入到表
                table.insertRow(row);

                affectedRows++;

            } catch (IllegalArgumentException e) {
                // 参数校验异常,直接抛出
                throw e;
            } catch (Exception e) {
                // 其他异常包装为RuntimeException
                throw new RuntimeException("Failed to insert row: " + e.getMessage(), e);
            }
        }

        return affectedRows;
    }

    /**
     * 构建列索引映射
     *
     * 如果指定了列名,返回这些列在表定义中的索引;
     * 否则返回null(表示按表定义顺序)。
     *
     * @param tableColumns 表的列定义
     * @param columnNames 指定的列名列表
     * @return 列索引数组,如果未指定列名返回null
     */
    private int[] buildColumnIndexes(List<Column> tableColumns, List<String> columnNames) {
        if (columnNames.isEmpty()) {
            return null; // 按表定义顺序
        }

        int[] indexes = new int[columnNames.size()];

        for (int i = 0; i < columnNames.size(); i++) {
            String colName = columnNames.get(i);
            int index = -1;

            // 查找列在表定义中的索引
            for (int j = 0; j < tableColumns.size(); j++) {
                if (tableColumns.get(j).getName().equalsIgnoreCase(colName)) {
                    index = j;
                    break;
                }
            }

            if (index == -1) {
                throw new IllegalArgumentException("Column not found: " + colName);
            }

            indexes[i] = index;
        }

        return indexes;
    }

    /**
     * 表达式求值
     *
     * 将Expression列表转换为实际值数组。
     * 注意:INSERT语句中的表达式通常是字面量(LiteralExpression)。
     *
     * @param expressions 表达式列表
     * @return 值数组
     */
    private Object[] evaluateExpressions(List<Expression> expressions) {
        Object[] values = new Object[expressions.size()];

        for (int i = 0; i < expressions.size(); i++) {
            Expression expr = expressions.get(i);
            // INSERT中的表达式通常是常量,不需要Row上下文
            // 使用eval()方法直接求值字面量
            if (expr.getType() == Expression.ExpressionType.LITERAL) {
                values[i] = evaluator.eval(expr, null, null);
            } else {
                throw new IllegalArgumentException(
                        "INSERT only supports literal expressions, got: " + expr.getType()
                );
            }
        }

        return values;
    }

    /**
     * 列顺序映射和类型转换
     *
     * 1. 如果指定了列名,按映射顺序排列值
     * 2. 类型检查和转换(确保值类型与列定义匹配)
     *
     * @param tableColumns 表的列定义
     * @param columnIndexes 列索引映射(为null表示按表定义顺序)
     * @param values 原始值数组
     * @return 映射和转换后的值数组
     */
    private Object[] mapAndConvertValues(List<Column> tableColumns,
                                          int[] columnIndexes,
                                          Object[] values) {
        // 目标值数组(按表定义顺序)
        Object[] result = new Object[tableColumns.size()];

        if (columnIndexes == null) {
            // 按表定义顺序插入
            if (values.length != tableColumns.size()) {
                throw new IllegalArgumentException(
                        "Column count doesn't match value count. " +
                                "Expected: " + tableColumns.size() + ", " +
                                "Actual: " + values.length
                );
            }

            for (int i = 0; i < values.length; i++) {
                result[i] = convertValue(values[i], tableColumns.get(i));
            }

        } else {
            // 按指定列名顺序插入
            if (values.length != columnIndexes.length) {
                throw new IllegalArgumentException(
                        "Column count doesn't match value count. " +
                                "Expected: " + columnIndexes.length + ", " +
                                "Actual: " + values.length
                );
            }

            // 先填充null
            for (int i = 0; i < result.length; i++) {
                result[i] = null;
            }

            // 按映射顺序填充
            for (int i = 0; i < columnIndexes.length; i++) {
                int targetIndex = columnIndexes[i];
                result[targetIndex] = convertValue(values[i], tableColumns.get(targetIndex));
            }
        }

        return result;
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
            throw new IllegalStateException("InsertOperator has not been executed yet");
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
        return "InsertOperator{" +
                "table=" + table.getTableName() +
                ", columnNames=" + columnNames +
                ", rowCount=" + values.size() +
                '}';
    }
}
