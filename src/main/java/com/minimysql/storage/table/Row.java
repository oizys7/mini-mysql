package com.minimysql.storage.table;

import java.util.Date;
import java.util.List;

/**
 * Row - 行数据
 *
 * Row表示表中的一行数据,提供简单的列值存储。
 *
 * 重构设计原则 (符合 MySQL 原理):
 * - Row 只存储数据(values),不存储结构定义(columns)
 * - 序列化/反序列化由 Table 负责(需要列定义)
 * - Row 是轻量级数据容器,职责单一
 * - 消除数据冗余:所有 Row 共享 Table 的 Column 定义
 *
 * 设计原则:
 * - 不可变对象(创建后不可修改)
 * - 简单的数据容器,没有复杂行为
 * - 不持有 Column 引用,避免数据冗余
 *
 * "Good taste": Row 只关心数据,Table 关心结构
 */
public class Row {

    /** 列值数组(null表示NULL) */
    private final Object[] values;

    /**
     * 创建行数据
     *
     * @param values 列值数组
     */
    public Row(Object[] values) {
        if (values == null) {
            throw new IllegalArgumentException("Values cannot be null");
        }

        this.values = values.clone();
    }

    /**
     * 创建行数据
     *
     * @param valueList 列值列表
     */
    public Row(List<Object> valueList) {
        if (valueList == null) {
            throw new IllegalArgumentException("Value list cannot be null");
        }

        this.values = valueList.toArray();
    }

    /**
     * 获取列数
     */
    public int getColumnCount() {
        return values.length;
    }

    /**
     * 获取列值(通过索引)
     *
     * @param index 列索引(从0开始)
     * @return 列值,可能为null
     */
    public Object getValue(int index) {
        if (index < 0 || index >= values.length) {
            throw new IndexOutOfBoundsException("Column index out of bounds: " + index);
        }
        return values[index];
    }

    /**
     * 获取列值(通过列名) - 已废弃
     *
     * ⚠️ 已废弃: Row 不再持有 Column 引用,请使用 Table.getRowValue() 代替
     * 此方法仅用于向后兼容,将在未来版本中移除
     *
     * @deprecated 请使用 Table.getRowValue(row, columnName) 代替
     * @param columnName 列名
     * @return 列值,可能为null
     */
    @Deprecated
    public Object getValue(String columnName) {
        throw new UnsupportedOperationException(
                "Row no longer supports column name lookup. " +
                "Please use Table.getRowValue(row, columnName) instead.");
    }

    /**
     * 获取INT类型列值
     */
    public Integer getInt(int index) {
        Object value = getValue(index);
        return (Integer) value;
    }

    /**
     * 获取BIGINT类型列值
     */
    public Long getLong(int index) {
        Object value = getValue(index);
        return (Long) value;
    }

    /**
     * 获取DOUBLE类型列值
     */
    public Double getDouble(int index) {
        Object value = getValue(index);
        return (Double) value;
    }

    /**
     * 获取BOOLEAN类型列值
     */
    public Boolean getBoolean(int index) {
        Object value = getValue(index);
        return (Boolean) value;
    }

    /**
     * 获取VARCHAR类型列值
     */
    public String getString(int index) {
        Object value = getValue(index);
        return (String) value;
    }

    /**
     * 获取DATE/TIMESTAMP类型列值
     */
    public Date getDate(int index) {
        Object value = getValue(index);
        return (Date) value;
    }

    /**
     * 检查列是否为NULL
     */
    public boolean isNull(int index) {
        return getValue(index) == null;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("Row{");
        for (int i = 0; i < values.length; i++) {
            if (i > 0) sb.append(", ");
            sb.append("col").append(i).append("=").append(values[i]);
        }
        sb.append("}");
        return sb.toString();
    }
}
