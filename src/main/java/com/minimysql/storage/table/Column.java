package com.minimysql.storage.table;

/**
 * Column - 列定义
 *
 * Column定义表中单个列的元数据,包括列名、数据类型、长度约束等。
 *
 * 设计原则:
 * - 不可变对象(创建后不可修改)
 * - 支持可空和非空列
 * - 变长类型需要指定最大长度
 * - 位置信息由Table管理,Column自身不存储
 *
 * 使用示例:
 * <pre>
 * Column nameCol = new Column("name", DataType.VARCHAR, 100, true);
 * Column ageCol = new Column("age", DataType.INT, 0, false);
 * </pre>
 *
 * "Good taste": 简单的数据对象,没有复杂的行为
 */
public class Column {

    /** 列名 */
    private final String name;

    /** 数据类型 */
    private final DataType type;

    /** 类型长度(仅对VARCHAR有效,表示最大字符数) */
    private final int length;

    /** 是否允许NULL */
    private final boolean nullable;

    /**
     * 创建列定义(非VARCHAR类型)
     *
     * @param name 列名
     * @param type 数据类型
     * @param nullable 是否允许NULL
     */
    public Column(String name, DataType type, boolean nullable) {
        this(name, type, 0, nullable);
    }

    /**
     * 创建列定义(VARCHAR类型)
     *
     * @param name 列名
     * @param type 数据类型
     * @param length 类型长度(仅VARCHAR有效)
     * @param nullable 是否允许NULL
     */
    public Column(String name, DataType type, int length, boolean nullable) {
        if (name == null || name.trim().isEmpty()) {
            throw new IllegalArgumentException("Column name cannot be null or empty");
        }

        if (type == null) {
            throw new IllegalArgumentException("Data type cannot be null");
        }

        // VARCHAR必须指定长度
        if (type == DataType.VARCHAR && length <= 0) {
            throw new IllegalArgumentException("VARCHAR type requires positive length");
        }

        // 非VARCHAR类型不应该指定长度
        if (type != DataType.VARCHAR && length != 0) {
            throw new IllegalArgumentException("Length is only valid for VARCHAR type");
        }

        this.name = name;
        this.type = type;
        this.length = length;
        this.nullable = nullable;
    }

    /**
     * 获取列名
     */
    public String getName() {
        return name;
    }

    /**
     * 获取数据类型
     */
    public DataType getType() {
        return type;
    }

    /**
     * 获取类型长度
     *
     * @return VARCHAR返回最大长度,其他类型返回0
     */
    public int getLength() {
        return length;
    }

    /**
     * 是否允许NULL
     */
    public boolean isNullable() {
        return nullable;
    }

    /**
     * 获取存储大小(字节)
     *
     * @return 固定大小类型返回实际大小,VARCHAR返回-1
     */
    public int getStorageSize() {
        return type.getStorageSize();
    }

    /**
     * 是否为变长类型
     */
    public boolean isVariableLength() {
        return type.isVariableLength();
    }

    /**
     * 验证值是否符合列约束
     *
     * @param value 要验证的值
     * @return 如果值有效返回true
     */
    public boolean validate(Object value) {
        // 检查NULL约束
        if (value == null) {
            return nullable;
        }

        // 委托给DataType验证
        return type.validate(value, length);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;

        Column column = (Column) obj;

        return name.equals(column.name);
    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }

    @Override
    public String toString() {
        return "Column{" +
                "name='" + name + '\'' +
                ", type=" + type +
                (type == DataType.VARCHAR ? "(" + length + ")" : "") +
                ", nullable=" + nullable +
                '}';
    }
}
