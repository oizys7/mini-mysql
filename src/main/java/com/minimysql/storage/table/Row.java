package com.minimysql.storage.table;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.List;

/**
 * Row - 行数据
 *
 * Row表示表中的一行数据,提供类型安全的列值访问。
 *
 * 行数据布局:
 * +------------------+ <- 0
 * | NULL Bitmap (n)  |  每列1bit,1表示NULL
 * +------------------+ <- ceil(colCount/8)
 * | Column 0 Value   |
 * | (fixed/var len)  |
 * +------------------+
 * | Column 1 Value   |
 * +------------------+
 * | ...              |
 * +------------------+
 *
 * NULL位图设计:
 * - 每列用1bit表示是否为NULL
 * - bit=1表示该列为NULL
 * - 位图大小: ceil(columnCount / 8) 字节
 * - 位图从低字节到高字节,每个字节内从低位到高位
 *
 * 设计原则:
 * - 不可变对象(创建后不可修改)
 * - 类型安全的get/set方法
 * - 支持所有DataType
 * - 支持NULL值
 *
 * "Good taste": 统一的序列化格式,没有特殊情况
 */
public class Row {

    /** 列定义列表 */
    private final List<Column> columns;

    /** 列值数组(null表示NULL) */
    private final Object[] values;

    /**
     * 创建行数据
     *
     * @param columns 列定义
     * @param values 列值数组(长度必须等于columns.size())
     */
    public Row(List<Column> columns, Object[] values) {
        if (columns == null || values == null) {
            throw new IllegalArgumentException("Columns and values cannot be null");
        }

        if (columns.size() != values.length) {
            throw new IllegalArgumentException(
                    "Column count mismatch: columns=" + columns.size() +
                    ", values=" + values.length);
        }

        this.columns = columns;
        this.values = values.clone();

        // 验证每个值
        for (int i = 0; i < columns.size(); i++) {
            Column col = columns.get(i);
            Object value = values[i];

            if (!col.validate(value)) {
                throw new IllegalArgumentException(
                        "Invalid value for column " + col.getName() + ": " + value);
            }
        }
    }

    /**
     * 获取列数
     */
    public int getColumnCount() {
        return columns.size();
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
     * 获取列值(通过列名)
     *
     * @param columnName 列名
     * @return 列值,可能为null
     */
    public Object getValue(String columnName) {
        int index = findColumnIndex(columnName);
        return getValue(index);
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

    /**
     * 查找列索引(通过列名)
     */
    private int findColumnIndex(String columnName) {
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i).getName().equalsIgnoreCase(columnName)) {
                return i;
            }
        }
        throw new IllegalArgumentException("Column not found: " + columnName);
    }

    /**
     * 序列化为字节数组
     *
     * 格式: NULL位图 + 列值数据
     *
     * @return 字节数组
     */
    public byte[] toBytes() {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();

            // 1. 写入NULL位图
            int nullBitmapSize = (columns.size() + 7) / 8;
            byte[] nullBitmap = new byte[nullBitmapSize];

            for (int i = 0; i < columns.size(); i++) {
                if (values[i] == null) {
                    int byteIndex = i / 8;
                    int bitIndex = i % 8;
                    nullBitmap[byteIndex] |= (1 << bitIndex);
                }
            }

            baos.write(nullBitmap);

            // 2. 写入每个列值
            ByteBuffer buffer;
            for (int i = 0; i < columns.size(); i++) {
                Object value = values[i];

                // NULL值跳过
                if (value == null) {
                    continue;
                }

                Column col = columns.get(i);
                DataType type = col.getType();

                switch (type) {
                    case INT:
                        buffer = ByteBuffer.allocate(4);
                        buffer.putInt((Integer) value);
                        baos.write(buffer.array());
                        break;

                    case BIGINT:
                        buffer = ByteBuffer.allocate(8);
                        buffer.putLong((Long) value);
                        baos.write(buffer.array());
                        break;

                    case DOUBLE:
                        buffer = ByteBuffer.allocate(8);
                        buffer.putDouble((Double) value);
                        baos.write(buffer.array());
                        break;

                    case BOOLEAN:
                        baos.write((Boolean) value ? 1 : 0);
                        break;

                    case VARCHAR:
                        String str = (String) value;
                        byte[] strBytes = str.getBytes(java.nio.charset.StandardCharsets.UTF_8);
                        buffer = ByteBuffer.allocate(2 + strBytes.length);
                        buffer.putShort((short) strBytes.length);
                        buffer.put(strBytes);
                        baos.write(buffer.array());
                        break;

                    case DATE:
                    case TIMESTAMP:
                        buffer = ByteBuffer.allocate(8);
                        buffer.putLong(((Date) value).getTime());
                        baos.write(buffer.array());
                        break;

                    default:
                        throw new IllegalArgumentException("Unsupported type: " + type);
                }
            }

            return baos.toByteArray();
        } catch (IOException e) {
            // ByteArrayOutputStream不应该抛出IOException,但编译器要求处理
            throw new RuntimeException("Failed to serialize row", e);
        }
    }

    /**
     * 从字节数组反序列化
     *
     * @param columns 列定义
     * @param data 字节数组
     * @return Row对象
     */
    public static Row fromBytes(List<Column> columns, byte[] data) {
        ByteBuffer buffer = ByteBuffer.wrap(data);
        Object[] values = new Object[columns.size()];

        // 1. 读取NULL位图
        int nullBitmapSize = (columns.size() + 7) / 8;
        byte[] nullBitmap = new byte[nullBitmapSize];
        buffer.get(nullBitmap);

        // 2. 读取每个列值
        for (int i = 0; i < columns.size(); i++) {
            // 检查是否为NULL
            int byteIndex = i / 8;
            int bitIndex = i % 8;
            boolean isNull = (nullBitmap[byteIndex] & (1 << bitIndex)) != 0;

            if (isNull) {
                values[i] = null;
                continue;
            }

            Column col = columns.get(i);
            DataType type = col.getType();

            switch (type) {
                case INT:
                    values[i] = buffer.getInt();
                    break;

                case BIGINT:
                    values[i] = buffer.getLong();
                    break;

                case DOUBLE:
                    values[i] = buffer.getDouble();
                    break;

                case BOOLEAN:
                    values[i] = buffer.get() == 1;
                    break;

                case VARCHAR:
                    int strLen = buffer.getShort() & 0xFFFF;
                    byte[] strBytes = new byte[strLen];
                    buffer.get(strBytes);
                    values[i] = new String(strBytes, java.nio.charset.StandardCharsets.UTF_8);
                    break;

                case DATE:
                case TIMESTAMP:
                    values[i] = new Date(buffer.getLong());
                    break;

                default:
                    throw new IllegalArgumentException("Unsupported type: " + type);
            }
        }

        return new Row(columns, values);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("Row{");
        for (int i = 0; i < values.length; i++) {
            if (i > 0) sb.append(", ");
            sb.append(columns.get(i).getName()).append("=").append(values[i]);
        }
        sb.append("}");
        return sb.toString();
    }
}
