package com.minimysql.executor.operator;

import com.minimysql.executor.ExpressionEvaluator;
import com.minimysql.executor.Operator;
import com.minimysql.parser.Expression;
import com.minimysql.storage.table.Column;
import com.minimysql.storage.table.Row;

import java.util.List;

/**
 * FilterOperator - WHERE条件过滤算子
 *
 * 包装子算子,过滤不符合WHERE条件的行。
 * 支持任意复杂的布尔表达式(AND/OR/NOT/比较运算)。
 *
 * 设计原则:
 * - "Good taste": 统一的过滤逻辑,不区分"简单条件"和"复杂条件"
 * - 责任链模式: 包装子Operator,形成处理管道
 * - 懒加载: 只在next()时才求值表达式,跳过不符合条件的行
 *
 * MySQL对应:
 * - MySQL中的WHERE条件过滤
 * - 对应EXPLAIN输出中的Filtered列
 *
 * 使用示例:
 * <pre>
 * Expression whereExpr = ...; // age > 18 AND name = 'Alice'
 * Operator scan = new ScanOperator(table);
 * Operator filter = new FilterOperator(scan, whereExpr, columns);
 *
 * while (filter.hasNext()) {
 *     Row row = filter.next();
 *     // row 一定满足 WHERE 条件
 * }
 * </pre>
 *
 * 数据流:
 * 子Operator → Row → ExpressionEvaluator.eval() → Boolean?
 * → true → 返回Row
 * → false → 跳过,继续hasNext()
 *
 * 性能特点:
 * - O(N)时间复杂度,需要检查每一行
 * - 提前终止:hasNext()可能需要多次调用子Operator.hasNext()/next()才能找到符合条件的行
 *
 * 实现细节:
 * - hasNext()会跳过所有不符合条件的行,直到找到符合条件的行或到达末尾
 * - next()直接返回hasNext()找到的符合条件的行
 * - 使用ExpressionEvaluator递归求值表达式树
 *
 * 设计哲学:
 * - "Bad programmers worry about the code. Good programmers worry about data structures."
 * - ExpressionEvaluator负责求值逻辑,FilterOperator负责过滤逻辑,职责分离
 * - 不实现"索引条件下推"(Index Condition Pushdown),保持简单
 */
public class FilterOperator implements Operator {

    /** 子算子(数据源) */
    private final Operator child;

    /** WHERE条件表达式 */
    private final Expression whereCondition;

    /** 列定义(用于表达式求值) */
    private final List<Column> columns;

    /** 表达式求值器 */
    private final ExpressionEvaluator evaluator;

    /** 当前行(缓存hasNext()找到的符合条件的行) */
    private Row currentRow;

    /** 是否已经找到下一行(用于hasNext()/next()协同) */
    private boolean hasNextRow;

    /**
     * 创建过滤算子
     *
     * @param child 子算子(数据源)
     * @param whereCondition WHERE条件表达式
     * @param columns 列定义(用于表达式求值)
     */
    public FilterOperator(Operator child, Expression whereCondition, List<Column> columns) {
        if (child == null) {
            throw new IllegalArgumentException("Child operator cannot be null");
        }
        if (whereCondition == null) {
            throw new IllegalArgumentException("WHERE condition cannot be null");
        }
        if (columns == null) {
            throw new IllegalArgumentException("Columns cannot be null");
        }

        this.child = child;
        this.whereCondition = whereCondition;
        this.columns = columns;
        this.evaluator = new ExpressionEvaluator();
        this.hasNextRow = false;
    }

    /**
     * 检查是否还有下一行(满足WHERE条件)
     *
     * 会跳过所有不符合条件的行,直到找到符合条件的行或到达末尾。
     *
     * @return 如果还有符合条件的行返回true
     */
    @Override
    public boolean hasNext() {
        // 如果已经找到下一行,直接返回
        if (hasNextRow) {
            return true;
        }

        // 遍历子算子,查找符合条件的行
        while (child.hasNext()) {
            Row row = child.next();

            // 求值WHERE条件
            Object result = evaluator.eval(whereCondition, row, columns);

            // 检查结果是否为Boolean
            if (!(result instanceof Boolean)) {
                throw new IllegalStateException(
                        "WHERE condition must evaluate to Boolean, got: " +
                                (result != null ? result.getClass().getSimpleName() : "null"));
            }

            boolean satisfies = (Boolean) result;

            if (satisfies) {
                // 找到符合条件的行,缓存并返回
                currentRow = row;
                hasNextRow = true;
                return true;
            }

            // 不符合条件,继续循环
        }

        // 没有找到符合条件的行
        return false;
    }

    /**
     * 获取下一行(满足WHERE条件)
     *
     * 调用前必须先调用hasNext()。
     *
     * @return 行数据
     * @throws java.util.NoSuchElementException 如果没有符合条件的行
     */
    @Override
    public Row next() {
        if (!hasNext()) {
            throw new java.util.NoSuchElementException("No more rows matching WHERE condition");
        }

        // 清除标记,返回缓存的行
        hasNextRow = false;
        return currentRow;
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
     * 获取WHERE条件表达式
     *
     * @return WHERE条件表达式
     */
    public Expression getWhereCondition() {
        return whereCondition;
    }

    @Override
    public String toString() {
        return "FilterOperator{" +
                "whereCondition=" + whereCondition +
                ", child=" + child +
                '}';
    }
}
