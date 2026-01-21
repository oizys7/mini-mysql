package com.minimysql.executor;

import com.minimysql.executor.operator.*;
import com.minimysql.metadata.SchemaManager;
import com.minimysql.parser.Statement;
import com.minimysql.parser.Statement.StatementType;
import com.minimysql.parser.statements.*;
import com.minimysql.storage.StorageEngine;
import com.minimysql.storage.table.Column;
import com.minimysql.storage.table.Table;

import java.util.ArrayList;
import java.util.List;

/**
 * ExecutionPlan - 查询执行计划
 *
 * 负责将Parser解析的Statement转换为Executor可执行的Operator树。
 *
 * 核心功能:
 * 1. 构建Operator树: 根据Statement类型生成对应的算子树
 * 2. 算子链组装: 将多个算子按照执行顺序连接起来
 * 3. 表名解析: 将表名解析为Table对象
 * 4. 表达式求值器注入: 为需要表达式的算子提供ExpressionEvaluator
 *
 * 设计原则:
 * - "Good taste": 简单直接的类型分发,没有复杂的if-else嵌套
 * - 工厂模式: 根据Statement类型创建对应的Operator树
 * - 单一职责: 只负责构建计划,不负责执行
 *
 * 数据流:
 * Statement → ExecutionPlan.build() → Operator树 → Executor.execute() → QueryResult
 *
 * MySQL对应:
 * - MySQL Optimizer的执行计划生成阶段
 * - EXPLAIN输出中的执行计划
 *
 * 使用示例:
 * <pre>
 * Statement stmt = parser.parse("SELECT * FROM users WHERE age > 18");
 * Operator plan = ExecutionPlan.build(stmt, storageEngine);
 *
 * // 执行计划
 * QueryResult result = new VolcanoExecutor(plan).executeQuery();
 * </pre>
 *
 * 设计哲学:
 * - "Bad programmers worry about the code. Good programmers worry about data structures."
 * - Statement和Operator都是数据结构,转换只是数据映射
 * - 不实现复杂的查询优化,保持简单直接
 */
public class ExecutionPlan {

    /**
     * 根据Statement构建Operator树
     *
     * 工厂方法,根据Statement类型分发到不同的构建逻辑。
     *
     * @param statement SQL语句
     * @param storageEngine 存储引擎
     * @return Operator树的根节点
     * @throws IllegalArgumentException 不支持的Statement类型
     */
    public static Operator build(Statement statement, StorageEngine storageEngine) {
        if (statement == null) {
            throw new IllegalArgumentException("Statement cannot be null");
        }

        if (storageEngine == null) {
            throw new IllegalArgumentException("StorageEngine cannot be null");
        }

        // 根据Statement类型分发
        switch (statement.getType()) {
            case SELECT:
                return buildSelectPlan((SelectStatement) statement, storageEngine);

            case INSERT:
                return buildInsertPlan((InsertStatement) statement, storageEngine);

            case UPDATE:
                return buildUpdatePlan((UpdateStatement) statement, storageEngine);

            case DELETE:
                return buildDeletePlan((DeleteStatement) statement, storageEngine);

            case CREATE_TABLE:
                return buildCreateTablePlan((CreateTableStatement) statement, storageEngine);

            case DROP_TABLE:
                return buildDropTablePlan((DropTableStatement) statement, storageEngine);

            default:
                throw new IllegalArgumentException(
                        "Unsupported statement type: " + statement.getType()
                );
        }
    }

    /**
     * 构建SELECT查询计划
     *
     * 执行计划: Scan → Filter → Project
     *
     * 设计原则:
     * - 简单直接: 不实现复杂的查询优化(如索引选择、谓词下推)
     * - 全表扫描: 始终使用ScanOperator扫描全表
     * - 先过滤后投影: 先执行WHERE条件过滤,再执行SELECT列投影
     *
     * @param statement SELECT语句
     * @param storageEngine 存储引擎
     * @return Operator树(Scan → Filter → Project)
     */
    private static Operator buildSelectPlan(SelectStatement statement, StorageEngine storageEngine) {
        // 1. 获取表对象
        String tableName = statement.getTableName();
        Table table = getTable(storageEngine, tableName);

        // 2. 创建ScanOperator(全表扫描)
        ScanOperator scan = new ScanOperator(table);

        // 3. 如果有WHERE条件,创建FilterOperator
        Operator source = scan;
        if (statement.getWhereClause().isPresent()) {
            FilterOperator filter = new FilterOperator(
                    scan,
                    statement.getWhereClause().get(),
                    table.getColumns()
            );
            source = filter;
        }

        // 4. 如果有SELECT列表,创建ProjectOperator
        // 注意: 如果SELECT *,则不需要ProjectOperator
        if (!statement.isSelectAll()) {
            ProjectOperator project = new ProjectOperator(
                    source,
                    statement.getSelectItems(),
                    table.getColumns()
            );
            source = project;
        }

        return source;
    }

    /**
     * 构建INSERT插入计划
     *
     * 执行计划: InsertOperator
     *
     * @param statement INSERT语句
     * @param storageEngine 存储引擎
     * @return InsertOperator
     */
    private static Operator buildInsertPlan(InsertStatement statement, StorageEngine storageEngine) {
        // 1. 获取表对象
        String tableName = statement.getTableName();
        Table table = getTable(storageEngine, tableName);

        // 2. 创建表达式求值器
        ExpressionEvaluator evaluator = new ExpressionEvaluator();

        // 3. 创建InsertOperator
        InsertOperator insert = new InsertOperator(
                table,
                statement.getColumnNames(),
                statement.getValues(),
                evaluator
        );

        return insert;
    }

    /**
     * 构建UPDATE更新计划
     *
     * 执行计划: UpdateOperator(内部使用全表扫描)
     *
     * @param statement UPDATE语句
     * @param storageEngine 存储引擎
     * @return UpdateOperator
     */
    private static Operator buildUpdatePlan(UpdateStatement statement, StorageEngine storageEngine) {
        // 1. 获取表对象
        String tableName = statement.getTableName();
        Table table = getTable(storageEngine, tableName);

        // 2. 创建表达式求值器
        ExpressionEvaluator evaluator = new ExpressionEvaluator();

        // 3. 创建UpdateOperator
        UpdateOperator update = new UpdateOperator(
                table,
                statement.getAssignments(),
                statement.getWhereClause().orElse(null),
                evaluator
        );

        return update;
    }

    /**
     * 构建DELETE删除计划
     *
     * 执行计划: DeleteOperator(内部使用全表扫描)
     *
     * @param statement DELETE语句
     * @param storageEngine 存储引擎
     * @return DeleteOperator
     */
    private static Operator buildDeletePlan(DeleteStatement statement, StorageEngine storageEngine) {
        // 1. 获取表对象
        String tableName = statement.getTableName();
        Table table = getTable(storageEngine, tableName);

        // 2. 创建表达式求值器
        ExpressionEvaluator evaluator = new ExpressionEvaluator();

        // 3. 创建DeleteOperator
        DeleteOperator delete = new DeleteOperator(
                table,
                statement.getWhereClause().orElse(null),
                evaluator
        );

        return delete;
    }

    /**
     * 构建CREATE TABLE计划
     *
     * 执行计划: CreateTableOperator
     *
     * @param statement CREATE TABLE语句
     * @param storageEngine 存储引擎
     * @return CreateTableOperator
     */
    private static Operator buildCreateTablePlan(CreateTableStatement statement, StorageEngine storageEngine) {
        // 1. 转换ColumnDefinition到Column
        List<Column> columns = convertColumns(statement.getColumns());

        // 2. 创建CreateTableOperator
        CreateTableOperator create = new CreateTableOperator(
                storageEngine,
                statement.getTableName(),
                columns
        );

        return create;
    }

    /**
     * 构建DROP TABLE计划
     *
     * 执行计划: DropTableOperator
     *
     * @param statement DROP TABLE语句
     * @param storageEngine 存储引擎
     * @return DropTableOperator
     */
    private static Operator buildDropTablePlan(DropTableStatement statement, StorageEngine storageEngine) {
        // 1. 创建DropTableOperator
        DropTableOperator drop = new DropTableOperator(
                storageEngine,
                statement.getTableName()
        );

        return drop;
    }

    /**
     * 从StorageEngine获取Table对象
     *
     * @param storageEngine 存储引擎
     * @param tableName 表名
     * @return Table对象
     * @throws IllegalArgumentException 表不存在
     */
    private static Table getTable(StorageEngine storageEngine, String tableName) {
        Table table = storageEngine.getTable(tableName);

        if (table == null) {
            throw new IllegalArgumentException("Table not found: " + tableName);
        }

        return table;
    }

    /**
     * 转换ColumnDefinition列表到Column列表
     *
     * @param columnDefinitions ColumnDefinition列表
     * @return Column列表
     */
    private static List<Column> convertColumns(List<ColumnDefinition> columnDefinitions) {
        List<Column> columns = new ArrayList<>();

        for (ColumnDefinition def : columnDefinitions) {
            Column column = new Column(
                    def.getColumnName(),
                    def.getDataType(),
                    def.getLength(),
                    def.isNullable()
            );
            columns.add(column);
        }

        return columns;
    }
}
