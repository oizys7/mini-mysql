package com.minimysql.executor;

import com.minimysql.executor.operator.FilterOperator;
import com.minimysql.executor.operator.ProjectOperator;
import com.minimysql.executor.operator.ScanOperator;
import com.minimysql.parser.Expression;
import com.minimysql.parser.Statement;
import com.minimysql.parser.statements.SelectStatement;
import com.minimysql.result.QueryResult;
import com.minimysql.storage.StorageEngine;
import com.minimysql.storage.table.Row;
import com.minimysql.storage.table.Table;

import java.util.ArrayList;
import java.util.List;

/**
 * VolcanoExecutor - 火山模型执行器
 *
 * 基于火山模型(Volcano Model)的SQL执行引擎,使用迭代器模式构建算子树。
 *
 * 核心概念:
 * - 每个算子实现 hasNext() + next() 接口
 * - 算子串联形成执行管道: Scan → Filter → Project
 * - 数据流从下往上,按需拉取(lazy evaluation)
 *
 * 设计原则:
 * - "Good taste": 统一的执行流程,不区分"简单查询"和"复杂查询"
 * - 责任链模式: 算子包装算子,形成处理管道
 * - 实用主义: 只支持最基本的 SELECT-WHERE 查询,不实现 JOIN/ORDER BY/GROUP BY
 *
 * MySQL对应:
 * - MySQL Executor中的执行引擎
 * - 对应EXPLAIN输出中的执行计划
 *
 * 使用示例:
 * <pre>
 * StorageEngine storageEngine = ...;
 * VolcanoExecutor executor = new VolcanoExecutor(storageEngine);
 *
 * Statement stmt = parser.parse("SELECT id, name FROM users WHERE age > 18");
 * QueryResult result = executor.execute(stmt);
 *
 * System.out.println(result);
 * </pre>
 *
 * 执行流程:
 * 1. 接收SelectStatement
 * 2. 从StorageEngine获取Table
 * 3. 构建算子树:
 *    - ScanOperator(table) - 全表扫描
 *    - FilterOperator(scan, where) - WHERE过滤(如果有)
 *    - ProjectOperator(filter, selectItems) - 列投影
 * 4. 遍历算子树,收集所有Row
 * 5. 封装为QueryResult返回
 *
 * 算子树示例:
 * <pre>
 * SQL: SELECT id, name FROM users WHERE age > 18
 *
 * 算子树:
 * ProjectOperator(selectItems=[id, name])
 *   └─ FilterOperator(whereCondition=age > 18)
 *       └─ ScanOperator(table=users)
 * </pre>
 *
 * 性能特点:
 * - 全表扫描: O(N)时间复杂度
 * - 按需拉取: 不会一次性加载所有数据到内存
 * - 算子开销: 每行数据需要经过多个算子处理
 *
 * 设计哲学:
 * - "Bad programmers worry about the code. Good programmers worry about data structures."
 * - 算子树本身就是数据结构,执行逻辑自然涌现
 * - 不实现查询优化器,保持简单
 * - 不实现向量化执行,保持简单
 * - 不实现并行执行,保持简单
 */
public class VolcanoExecutor {

    /** 存储引擎(用于获取表) */
    private final StorageEngine storageEngine;

    /**
     * 创建火山模型执行器
     *
     * @param storageEngine 存储引擎
     */
    public VolcanoExecutor(StorageEngine storageEngine) {
        if (storageEngine == null) {
            throw new IllegalArgumentException("StorageEngine cannot be null");
        }
        this.storageEngine = storageEngine;
    }

    /**
     * 执行SQL语句
     *
     * 当前只支持SELECT语句,其他语句类型抛出UnsupportedOperationException。
     *
     * @param statement SQL语句
     * @return 查询结果集
     * @throws ExecutionException 执行失败
     */
    public QueryResult execute(Statement statement) {
        if (statement == null) {
            throw new IllegalArgumentException("Statement cannot be null");
        }

        // 只支持SELECT语句
        if (statement.getType() != Statement.StatementType.SELECT) {
            throw new UnsupportedOperationException(
                    "Only SELECT statements are supported, got: " + statement.getType());
        }

        return executeSelect((SelectStatement) statement);
    }

    /**
     * 执行SELECT查询
     *
     * @param selectStatement SELECT语句
     * @return 查询结果集
     */
    private QueryResult executeSelect(SelectStatement selectStatement) {
        // 1. 获取表
        String tableName = selectStatement.getTableName();
        Table table = storageEngine.getTable(tableName);
        if (table == null) {
            throw new ExecutionException("Table not found: " + tableName);
        }

        // 2. 构建算子树
        com.minimysql.executor.Operator operator = buildOperatorTree(selectStatement, table);

        // 3. 执行查询,收集所有行
        List<Row> rows = new ArrayList<>();
        while (operator.hasNext()) {
            Row row = operator.next();
            rows.add(row);
        }

        // 4. 获取列定义(投影后的列)
        List<com.minimysql.storage.table.Column> columns;
        if (operator instanceof ProjectOperator) {
            ProjectOperator projectOp = (ProjectOperator) operator;
            columns = projectOp.getProjectedColumns();
        } else {
            // 没有ProjectOperator,说明是SELECT *,使用表的所有列
            columns = table.getColumns();
        }

        // 5. 封装为QueryResult
        return new QueryResult(columns, rows);
    }

    /**
     * 构建算子树
     *
     * 算子树结构(从下往上):
     * ScanOperator → FilterOperator (可选) → ProjectOperator (可选)
     *
     * @param selectStatement SELECT语句
     * @param table 表对象
     * @return 算子树的根节点
     */
    private com.minimysql.executor.Operator buildOperatorTree(
            SelectStatement selectStatement,
            Table table) {

        // 1. 创建ScanOperator(叶子节点)
        ScanOperator scan = new ScanOperator(table);
        com.minimysql.executor.Operator current = scan;

        // 2. 如果有WHERE条件,创建FilterOperator
        if (selectStatement.getWhereClause().isPresent()) {
            Expression whereCondition = selectStatement.getWhereClause().get();
            FilterOperator filter = new FilterOperator(
                    current,
                    whereCondition,
                    table.getColumns()
            );
            current = filter;
        }

        // 3. 如果不是SELECT *,创建ProjectOperator
        if (!selectStatement.isSelectAll()) {
            ProjectOperator project = new ProjectOperator(
                    current,
                    selectStatement.getSelectItems(),
                    table.getColumns()
            );
            current = project;
        }

        // 4. 返回算子树的根节点
        return current;
    }

    /**
     * 执行异常
     */
    public static class ExecutionException extends RuntimeException {
        public ExecutionException(String message) {
            super(message);
        }

        public ExecutionException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
