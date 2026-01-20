package com.minimysql.executor.operator;

import com.minimysql.executor.ExpressionEvaluator;
import com.minimysql.executor.Operator;
import com.minimysql.parser.Expression;
import com.minimysql.parser.expressions.ColumnExpression;
import com.minimysql.storage.table.Column;
import com.minimysql.storage.table.DataType;
import com.minimysql.storage.table.Row;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * ProjectOperator - 列投影算子
 *
 * 实现SELECT子句的列投影功能,支持:
 * - SELECT * - 返回所有列
 * - SELECT col1, col2 - 返回指定列
 *
 * 设计原则:
 * - "Good taste": 统一的投影逻辑,不区分"全列"和"指定列"
 * - 责任链模式: 包装子Operator,形成处理管道
 * - 不可变性: 不修改原始Row,创建新的Row对象
 *
 * MySQL对应:
 * - MySQL中的SELECT列表投影
 * - 对应临时表(TempTable)的构建
 *
 * 使用示例:
 * <pre>
 * // SELECT id, name FROM users
 * List<Expression> selectItems = Arrays.asList(
 *     new ColumnExpression("id"),
 *     new ColumnExpression("name")
 * );
 * Operator scan = new ScanOperator(table);
 * Operator project = new ProjectOperator(scan, selectItems, columns);
 *
 * while (project.hasNext()) {
 *     Row row = project.next();
 *     // row 只包含 id 和 name 两列
 * }
 * </pre>
 *
 * 数据流:
 * 子Operator → Row(full columns) → 提取指定列 → NewRow(projected columns)
 *
 * 性能特点:
 * - O(N)时间复杂度,需要处理每一行
 * - 内存开销:创建新的Row对象(不可变设计)
 *
 * 实现细节:
 * - SELECT *:直接返回子Operator的Row,不创建新对象
 * - SELECT col1, col2:创建新Row,只包含指定列
 * - 支持表达式(未来扩展):SELECT age + 1 AS new_age
 *
 * 设计哲学:
 * - "Bad programmers worry about the code. Good programmers worry about data structures."
 * - SELECT * 直接返回原Row,零拷贝,最高效
 * - SELECT 指定列创建新Row,类型安全,符合SQL语义
 * - 不实现复杂的表达式投影(如 SELECT age + 1),保持简单
 */
public class ProjectOperator implements Operator {

    /** 子算子(数据源) */
    private final Operator child;

    /** SELECT列表(空表示SELECT *) */
    private final List<Expression> selectItems;

    /** 投影后的列定义 */
    private final List<Column> projectedColumns;

    /** 原始列定义(用于表达式求值) */
    private final List<Column> originalColumns;

    /** 是否为SELECT * */
    private final boolean isSelectAll;

    /** 表达式求值器(用于表达式投影) */
    private final ExpressionEvaluator evaluator;

    /**
     * 创建投影算子
     *
     * @param child 子算子(数据源)
     * @param selectItems SELECT列表(空表示SELECT *)
     * @param originalColumns 原始列定义(用于表达式求值)
     */
    public ProjectOperator(Operator child, List<Expression> selectItems, List<Column> originalColumns) {
        if (child == null) {
            throw new IllegalArgumentException("Child operator cannot be null");
        }
        if (originalColumns == null) {
            throw new IllegalArgumentException("Original columns cannot be null");
        }

        this.child = child;
        this.selectItems = selectItems != null ? List.copyOf(selectItems) : List.of();
        this.originalColumns = originalColumns;
        this.isSelectAll = this.selectItems.isEmpty();
        this.evaluator = new ExpressionEvaluator();

        // 计算投影后的列定义
        if (isSelectAll) {
            // SELECT *:投影后的列定义 = 原始列定义
            this.projectedColumns = List.copyOf(originalColumns);
        } else {
            // SELECT col1, col2:根据selectItems构建列定义
            this.projectedColumns = buildProjectedColumns();
        }
    }

    /**
     * 构建投影后的列定义
     *
     * @return 列定义列表
     */
    private List<Column> buildProjectedColumns() {
        List<Column> columns = new ArrayList<>();

        for (Expression item : selectItems) {
            if (item.getType() == Expression.ExpressionType.COLUMN) {
                // 列引用:从原始列定义中查找
                ColumnExpression colExpr = (ColumnExpression) item;
                String columnName = colExpr.getColumnName();

                Column originalColumn = findColumn(columnName);
                if (originalColumn == null) {
                    throw new IllegalArgumentException("Column not found: " + columnName);
                }

                columns.add(originalColumn);
            } else {
                // 表达式(如 age + 1):创建临时列
                // 暂不支持,抛异常
                throw new UnsupportedOperationException(
                        "Expression projection not supported yet: " + item);
            }
        }

        return columns;
    }

    /**
     * 查找列定义
     *
     * @param columnName 列名
     * @return 列定义,如果不存在返回null
     */
    private Column findColumn(String columnName) {
        for (Column col : originalColumns) {
            if (col.getName().equalsIgnoreCase(columnName)) {
                return col;
            }
        }
        return null;
    }

    /**
     * 检查是否还有下一行
     *
     * @return 如果还有下一行返回true
     */
    @Override
    public boolean hasNext() {
        return child.hasNext();
    }

    /**
     * 获取下一行(应用投影)
     *
     * 如果是SELECT *,直接返回子Operator的Row。
     * 如果是SELECT指定列,创建新的Row对象,只包含指定列。
     *
     * @return 行数据
     * @throws java.util.NoSuchElementException 如果没有下一行
     */
    @Override
    public Row next() {
        Row originalRow = child.next();

        if (isSelectAll) {
            // SELECT *:直接返回原Row,不创建新对象
            return originalRow;
        }

        // SELECT col1, col2:创建新Row,只包含指定列
        return projectRow(originalRow);
    }

    /**
     * 投影行数据
     *
     * 从原始行中提取指定列的值,创建新的Row对象。
     *
     * @param originalRow 原始行
     * @return 投影后的行
     */
    private Row projectRow(Row originalRow) {
        List<Object> projectedValues = new ArrayList<>();

        for (Expression item : selectItems) {
            if (item.getType() == Expression.ExpressionType.COLUMN) {
                // 列引用:直接从原Row中获取值
                ColumnExpression colExpr = (ColumnExpression) item;
                String columnName = colExpr.getColumnName();
                Object value = originalRow.getValue(columnName);
                projectedValues.add(value);
            } else {
                // 表达式:求值后添加
                Object value = evaluator.eval(item, originalRow, originalColumns);
                projectedValues.add(value);
            }
        }

        return new Row(projectedColumns, projectedValues.toArray());
    }

    /**
     * 获取子算子
     *
     * @return 子算子
     */
    public Operator getChild() {
        return child;
    }

    /**
     * 获取SELECT列表
     *
     * @return SELECT列表(空表示SELECT *)
     */
    public List<Expression> getSelectItems() {
        return selectItems;
    }

    /**
     * 获取投影后的列定义
     *
     * @return 列定义列表
     */
    public List<Column> getProjectedColumns() {
        return projectedColumns;
    }

    /**
     * 判断是否为SELECT *
     *
     * @return 如果是SELECT *返回true
     */
    public boolean isSelectAll() {
        return isSelectAll;
    }

    @Override
    public String toString() {
        return "ProjectOperator{" +
                "selectItems=" + (isSelectAll ? "*" : selectItems) +
                ", child=" + child +
                '}';
    }
}
