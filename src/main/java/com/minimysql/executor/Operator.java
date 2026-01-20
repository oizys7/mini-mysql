package com.minimysql.executor;

import com.minimysql.storage.table.Row;

/**
 * Operator - 执行算子接口
 *
 * 定义所有执行算子的统一接口,基于火山模型(Volcano Model)的迭代器模式。
 *
 * 核心概念:
 * - 每个算子实现 hasNext() + next() 方法
 * - 算子可以串联形成执行管道(Operator Tree)
 * - 数据流从下往上: Scan → Filter → Project
 *
 * 设计原则:
 * - "Good taste": 所有算子统一接口,消除if-else判断
 * - 迭代器模式: 按需拉取数据,内存友好
 * - 可组合: 任何算子都可以包装另一个算子
 *
 * MySQL对应:
 * - MySQL Executor中的迭代器接口
 * - 火山模型是数据库执行引擎的经典模型
 *
 * 使用示例:
 * <pre>
 * // 构建算子树
 * Operator scan = new ScanOperator(table);
 * Operator filter = new FilterOperator(scan, whereExpr, columns);
 * Operator project = new ProjectOperator(filter, selectItems);
 *
 * // 执行查询
 * while (project.hasNext()) {
 *     Row row = project.next();
 *     // 处理行数据
 * }
 * </pre>
 *
 * 设计哲学:
 * - "Bad programmers worry about the code. Good programmers worry about data structures."
 * - 算子树本身就是数据结构,执行逻辑自然涌现
 * - 消除特殊情况: 不区分"简单查询"和"复杂查询"
 */
public interface Operator {

    /**
     * 检查是否还有下一行数据
     *
     * @return 如果还有下一行返回true,否则返回false
     */
    boolean hasNext();

    /**
     * 获取下一行数据
     *
     * 调用前必须先调用hasNext()检查。
     *
     * @return 行数据
     * @throws java.util.NoSuchElementException 如果没有下一行
     */
    Row next();

    /**
     * 重置算子状态(可选实现)
     *
     * 将算子重置到初始状态,可以重新遍历数据。
     * 默认实现不支持重置,抛出UnsupportedOperationException。
     */
    default void reset() {
        throw new UnsupportedOperationException("Reset not supported");
    }

    /**
     * 关闭算子(可选实现)
     *
     * 释放算子持有的资源(如打开的文件、数据库连接等)。
     * 默认实现不做什么。
     */
    default void close() {
        // 默认不做什么
    }
}
