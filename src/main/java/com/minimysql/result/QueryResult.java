package com.minimysql.result;

import com.minimysql.storage.table.Column;
import com.minimysql.storage.table.Row;

import java.util.List;

/**
 * QueryResult - 查询结果集
 *
 * 封装查询执行的结果,包括列定义和行数据。
 * 提供格式化输出功能,便于调试和展示。
 *
 * 设计原则:
 * - "Good taste": 简单的数据容器,没有复杂逻辑
 * - 不可变性:创建后不可修改
 * - 实用主义:提供toString()格式化输出
 *
 * 使用示例:
 * <pre>
 * List<Column> columns = ...;
 * List<Row> rows = ...;
 * QueryResult result = new QueryResult(columns, rows);
 *
 * // 打印结果集
 * System.out.println(result);
 * </pre>
 *
 * 输出示例:
 * <pre>
 * +----+-------+-----+
 * | id | name  | age |
 * +----+-------+-----+
 * | 1  | Alice | 25  |
 * | 2  | Bob   | 30  |
 * +----+-------+-----+
 * 2 rows in set
 * </pre>
 *
 * 设计哲学:
 * - "Bad programmers worry about the code. Good programmers worry about data structures."
 * - QueryResult就是数据结构,格式化输出自然涌现
 * - 不实现ResultSet接口(不兼容JDBC),保持简单
 */
public class QueryResult {

    /** 列定义 */
    private final List<Column> columns;

    /** 行数据 */
    private final List<Row> rows;

    /**
     * 创建查询结果集
     *
     * @param columns 列定义
     * @param rows 行数据
     */
    public QueryResult(List<Column> columns, List<Row> rows) {
        if (columns == null) {
            throw new IllegalArgumentException("Columns cannot be null");
        }
        if (rows == null) {
            throw new IllegalArgumentException("Rows cannot be null");
        }

        this.columns = List.copyOf(columns);
        this.rows = List.copyOf(rows);
    }

    /**
     * 获取列定义
     *
     * @return 列定义列表
     */
    public List<Column> getColumns() {
        return columns;
    }

    /**
     * 获取行数据
     *
     * @return 行数据列表
     */
    public List<Row> getRows() {
        return rows;
    }

    /**
     * 获取行数
     *
     * @return 行数
     */
    public int getRowCount() {
        return rows.size();
    }

    /**
     * 获取列数
     *
     * @return 列数
     */
    public int getColumnCount() {
        return columns.size();
    }

    /**
     * 格式化输出为表格
     *
     * @return 格式化后的表格字符串
     */
    @Override
    public String toString() {
        if (rows.isEmpty()) {
            return "Empty set (0.00 sec)";
        }

        // 计算每列的最大宽度
        int[] columnWidths = new int[columns.size()];
        for (int i = 0; i < columns.size(); i++) {
            columnWidths[i] = columns.get(i).getName().length();
        }

        for (Row row : rows) {
            for (int i = 0; i < columns.size(); i++) {
                Object value = row.getValue(i);
                String strValue = value != null ? value.toString() : "NULL";
                columnWidths[i] = Math.max(columnWidths[i], strValue.length());
            }
        }

        // 构建分隔线
        String separator = buildSeparator(columnWidths);

        // 构建表头
        StringBuilder sb = new StringBuilder();
        sb.append(separator).append("\n");
        sb.append("| ");
        for (int i = 0; i < columns.size(); i++) {
            sb.append(padRight(columns.get(i).getName(), columnWidths[i]));
            sb.append(" | ");
        }
        sb.append("\n");
        sb.append(separator).append("\n");

        // 构建数据行
        for (Row row : rows) {
            sb.append("| ");
            for (int i = 0; i < columns.size(); i++) {
                Object value = row.getValue(i);
                String strValue = value != null ? value.toString() : "NULL";
                sb.append(padRight(strValue, columnWidths[i]));
                sb.append(" | ");
            }
            sb.append("\n");
        }
        sb.append(separator).append("\n");

        // 添加行数统计
        sb.append(rows.size()).append(" row").append(rows.size() > 1 ? "s" : "").append(" in set");

        return sb.toString();
    }

    /**
     * 构建分隔线
     *
     * @param columnWidths 列宽度数组
     * @return 分隔线字符串
     */
    private String buildSeparator(int[] columnWidths) {
        StringBuilder sb = new StringBuilder("+");
        for (int width : columnWidths) {
            sb.append("-".repeat(width + 2));
            sb.append("+");
        }
        return sb.toString();
    }

    /**
     * 右填充字符串到指定宽度
     *
     * @param str 字符串
     * @param width 目标宽度
     * @return 填充后的字符串
     */
    private String padRight(String str, int width) {
        if (str.length() >= width) {
            return str;
        }
        return str + " ".repeat(width - str.length());
    }
}
