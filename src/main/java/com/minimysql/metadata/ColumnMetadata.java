package com.minimysql.metadata;

import com.minimysql.storage.table.DataType;

import java.util.Objects;

/**
 * ColumnMetadata - 列元数据
 *
 * 在内存中表示单个列的元数据定义。
 *
 * "Good taste": 简单的数据对象，没有复杂的行为
 */
public class ColumnMetadata {

    /** 表ID */
    private final int tableId;

    /** 列名 */
    private final String name;

    /** 数据类型 */
    private final DataType type;

    /** 类型长度(仅VARCHAR有效) */
    private final int length;

    /** 是否允许NULL */
    private final boolean nullable;

    /** 列位置(从0开始) */
    private final int position;

    /**
     * 创建列元数据
     *
     * @param tableId 表ID
     * @param name 列名
     * @param type 数据类型
     * @param length 类型长度
     * @param nullable 是否可空
     * @param position 列位置
     */
    public ColumnMetadata(int tableId, String name, DataType type, int length, boolean nullable, int position) {
        this.tableId = tableId;
        this.name = name;
        this.type = type;
        this.length = length;
        this.nullable = nullable;
        this.position = position;
    }

    public int getTableId() {
        return tableId;
    }

    public String getName() {
        return name;
    }

    public DataType getType() {
        return type;
    }

    public int getLength() {
        return length;
    }

    public boolean isNullable() {
        return nullable;
    }

    public int getPosition() {
        return position;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        ColumnMetadata that = (ColumnMetadata) obj;
        return tableId == that.tableId && position == that.position && Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableId, name, position);
    }

    @Override
    public String toString() {
        return "ColumnMetadata{" +
                "tableId=" + tableId +
                ", name='" + name + '\'' +
                ", type=" + type +
                (type == DataType.VARCHAR ? "(" + length + ")" : "") +
                ", nullable=" + nullable +
                ", position=" + position +
                '}';
    }
}
