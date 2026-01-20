package com.minimysql.parser.statements;

import com.minimysql.storage.table.DataType;

/**
 * ColumnDefinition - 列定义(用于CREATE TABLE)
 *
 * 表示CREATE TABLE语句中的单个列定义。
 *
 * 设计原则:
 * - 简单的数据对象(POJO)
 * - 不可变对象(创建后不可修改)
 * - 映射到存储层的Column类
 */
public class ColumnDefinition {

    /** 列名 */
    private final String columnName;

    /** 数据类型 */
    private final DataType dataType;

    /** 类型长度(仅VARCHAR有效) */
    private final int length;

    /** 是否允许NULL */
    private final boolean nullable;

    public ColumnDefinition(String columnName, DataType dataType, int length, boolean nullable) {
        this.columnName = columnName;
        this.dataType = dataType;
        this.length = length;
        this.nullable = nullable;
    }

    public String getColumnName() {
        return columnName;
    }

    public DataType getDataType() {
        return dataType;
    }

    public int getLength() {
        return length;
    }

    public boolean isNullable() {
        return nullable;
    }

    @Override
    public String toString() {
        return "ColumnDefinition{" +
                "columnName='" + columnName + '\'' +
                ", dataType=" + dataType +
                (dataType == DataType.VARCHAR ? "(" + length + ")" : "") +
                ", nullable=" + nullable +
                '}';
    }
}
