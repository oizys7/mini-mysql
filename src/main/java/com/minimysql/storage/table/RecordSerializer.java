package com.minimysql.storage.table;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.List;

/**
 * RecordSerializer - InnoDB 行格式序列化器
 *
 * 实现 InnoDB 的 COMPACT 行格式，这是 MySQL 5.0+ 的默认格式。
 *
 * <p>InnoDB COMPACT 行格式结构:
 * <pre>
 * +------------------------------+ <- 0
 * | NULL 标记位图 (1-2 bytes)     |  可为 NULL 的列需要标记
 * | - bit 1 = NULL, bit 0 = NOT NULL|
 * | - 按列顺序排列                |
 * +------------------------------+ <- bitmap
 * | 变长字段长度列表 (1-2 bytes)  |  只包含非NULL的变长字段 (VARCHAR)
 * | - 每个变长字段 1-2 字节        |
 * | - 按列顺序逆序排列             |
 * +------------------------------+ <- bitmap + var_lengths
 * | 记录头信息 (5 bytes)          |  简化实现，不包含完整头信息
 * | (可选，用于事务和 MVCC)        |  生产环境需要: DB_TRX_ID, DB_ROLL_PTR
 * +------------------------------+ <- bitmap + var_lengths + 5
 * | 列数据                        |  按列顺序存储
 * | - Column 0                   |  - 固定长度类型: INT(4), BIGINT(8), DOUBLE(8)
 * | - Column 1                   |  - 变长类型: VARCHAR(len)
 * | - ...                        |  - NULL 值不占空间
 * +------------------------------+
 * </pre>
 *
 * <p>重要: 为了符合MySQL InnoDB格式,顺序调整为:
 * 1. NULL bitmap (先读取,因为大小可根据表定义计算)
 * 2. 变长字段长度列表 (根据NULL bitmap跳过NULL的VARCHAR)
 * 3. 列数据
 *
 * <p>参考文档:
 * <ul>
 *   <li>https://dev.mysql.com/doc/refman/8.0/en/innodb-row-format.html</li>
 *   <li>https://dev.mysql.com/doc/refman/8.0/en/innodb-page-structure.html</li>
 * </ul>
 *
 * <p>设计原则:
 * <ul>
 *   <li>符合 MySQL InnoDB COMPACT 行格式规范</li>
 *   <li>支持所有数据类型: INT, BIGINT, DOUBLE, BOOLEAN, VARCHAR, DATE, TIMESTAMP</li>
 *   <li>正确处理 NULL 值 (NULL 列不占数据空间)</li>
 *   <li>高效序列化/反序列化</li>
 * </ul>
 *
 * @author Mini MySQL Project
 * @see Row
 * @see Column
 */
public class RecordSerializer {

    /**
     * 变长字段长度阈值 (超过 255 字节需要 2 字节存储长度)
     */
    private static final int VARCHAR_LENGTH_THRESHOLD = 127;

    /**
     * 将逻辑 Row 序列化为 InnoDB COMPACT 格式的物理记录
     *
     * <p>序列化流程:
     * <ol>
     *   <li>写入变长字段长度列表 (只包含 VARCHAR 类型)</li>
     *   <li>写入 NULL 标记位图</li>
     *   <li>写入列数据 (按列顺序，NULL 列跳过)</li>
     * </ol>
     *
     * @param row    逻辑行数据
     * @param columns 列定义列表
     * @return 物理记录 (字节数组)
     * @throws IllegalArgumentException 如果参数无效
     */
    public static byte[] serialize(Row row, List<Column> columns) {
        if (row == null) {
            throw new IllegalArgumentException("Row cannot be null");
        }
        if (columns == null || columns.isEmpty()) {
            throw new IllegalArgumentException("Columns cannot be null or empty");
        }
        if (row.getColumnCount() != columns.size()) {
            throw new IllegalArgumentException(
                    "Row column count (" + row.getColumnCount() +
                    ") does not match table column count (" + columns.size() + ")");
        }

        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();

            // 第 1 步: 写入 NULL 标记位图 (MySQL格式: NULL bitmap在前)
            byte[] nullBitmap = serializeNullBitmap(row, columns);
            baos.write(nullBitmap);

            // 第 2 步: 写入变长字段长度列表 (只包含非NULL的VARCHAR)
            byte[] variableLengths = serializeVariableLengths(row, columns);
            baos.write(variableLengths);

            // 第 3 步: 写入列数据
            byte[] columnData = serializeColumnData(row, columns);
            baos.write(columnData);

            return baos.toByteArray();
        } catch (IOException e) {
            // ByteArrayOutputStream 不应该抛出 IOException，但编译器要求处理
            throw new RuntimeException("Failed to serialize row", e);
        }
    }

    /**
     * 从物理记录反序列化为逻辑 Row
     *
     * <p>反序列化流程:
     * <ol>
     *   <li>读取变长字段长度列表</li>
     *   <li>读取 NULL 标记位图</li>
     *   <li>根据列定义和位图，逐列读取数据</li>
     * </ol>
     *
     * @param record  物理记录 (字节数组)
     * @param columns 列定义列表
     * @return 逻辑行数据
     * @throws IllegalArgumentException 如果参数无效或记录格式错误
     */
    public static Row deserialize(byte[] record, List<Column> columns) {
        if (record == null || record.length == 0) {
            throw new IllegalArgumentException("Record cannot be null or empty");
        }
        if (columns == null || columns.isEmpty()) {
            throw new IllegalArgumentException("Columns cannot be null or empty");
        }

        ByteBuffer buffer = ByteBuffer.wrap(record);
        Object[] values = new Object[columns.size()];

        // 第 1 步: 读取 NULL 标记位图 (MySQL格式: NULL bitmap在前)
        boolean[] nullBitmap = deserializeNullBitmap(buffer, columns);

        // 第 2 步: 读取变长字段长度列表 (根据NULL bitmap跳过NULL的VARCHAR)
        int[] variableLengths = deserializeVariableLengths(buffer, columns, nullBitmap);

        // 第 3 步: 读取列数据
        for (int i = 0; i < columns.size(); i++) {
            // 检查是否为 NULL
            if (nullBitmap[i]) {
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
                    int strLen = variableLengths[i];
                    byte[] strBytes = new byte[strLen];
                    buffer.get(strBytes);
                    values[i] = new String(strBytes, StandardCharsets.UTF_8);
                    break;

                case DATE:
                case TIMESTAMP:
                    values[i] = new Date(buffer.getLong());
                    break;

                default:
                    throw new IllegalArgumentException("Unsupported data type: " + type);
            }
        }

        return new Row(values);
    }

    /**
     * 计算记录的物理存储大小
     *
     * <p>用于页空间检查和数据页插入前的预判断。
     *
     * @param row      逻辑行数据
     * @param columns  列定义列表
     * @return 字节数
     */
    public static int calculateRecordSize(Row row, List<Column> columns) {
        int size = 0;

        // 1. NULL 标记位图 (MySQL格式: NULL bitmap在前)
        size += (columns.size() + 7) / 8;

        // 2. 变长字段长度列表 (每个非NULL的VARCHAR 1-2 字节)
        for (int i = 0; i < columns.size(); i++) {
            Column col = columns.get(i);
            if (col.getType() == DataType.VARCHAR) {
                Object value = row.getValue(i);
                if (value != null) {
                    int strLen = value.toString().getBytes(StandardCharsets.UTF_8).length;
                    size += (strLen > VARCHAR_LENGTH_THRESHOLD) ? 2 : 1;
                }
                // NULL的VARCHAR不记录长度,跳过
            }
        }

        // 3. 列数据
        for (int i = 0; i < columns.size(); i++) {
            Object value = row.getValue(i);
            if (value == null) {
                continue; // NULL 值不占空间
            }

            Column col = columns.get(i);
            DataType type = col.getType();

            switch (type) {
                case INT:
                    size += 4;
                    break;
                case BIGINT:
                    size += 8;
                    break;
                case DOUBLE:
                    size += 8;
                    break;
                case BOOLEAN:
                    size += 1;
                    break;
                case VARCHAR:
                    String str = value.toString();
                    // VARCHAR长度已在变长长度列表中计算,这里只计算实际数据
                    size += str.getBytes(StandardCharsets.UTF_8).length;
                    break;
                case DATE:
                case TIMESTAMP:
                    size += 8;
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported data type: " + type);
            }
        }

        return size;
    }

    // ==================== 私有辅助方法 ====================

    /**
     * 序列化变长字段长度列表
     *
     * <p>InnoDB COMPACT 格式:
     * - 只包含变长字段 (VARCHAR)
     * - 按列顺序逆序排列
     * - 每个长度 1-2 字节 (<=127 用 1 字节, >127 用 2 字节)
     *
     * @param row      逻辑行数据
     * @param columns  列定义列表
     * @return 变长字段长度列表 (字节数组)
     */
    private static byte[] serializeVariableLengths(Row row, List<Column> columns) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        // 按列顺序逆序排列 (MySQL 特性)
        for (int i = columns.size() - 1; i >= 0; i--) {
            Column col = columns.get(i);
            if (col.getType() != DataType.VARCHAR) {
                continue; // 只处理变长字段
            }

            Object value = row.getValue(i);
            if (value == null) {
                continue; // NULL 值不记录长度
            }

            String str = value.toString();
            int strLen = str.getBytes(StandardCharsets.UTF_8).length;

            if (strLen > VARCHAR_LENGTH_THRESHOLD) {
                // 长度 > 127，使用 2 字节
                // 第一字节: 高位置1 + 长度高7位
                baos.write(0x80 | ((strLen >> 8) & 0x7F));
                // 第二字节: 长度低8位
                baos.write(strLen & 0xFF);
            } else {
                // 长度 <= 127，使用 1 字节
                baos.write(strLen);
            }
        }

        return baos.toByteArray();
    }

    /**
     * 反序列化变长字段长度列表
     *
     * <p>MySQL InnoDB 兼容实现:
     * - 只读取非NULL的VARCHAR字段的长度
     * - NULL的VARCHAR字段不记录长度,跳过
     * - 按列顺序逆序读取
     *
     * @param buffer     ByteBuffer
     * @param columns    列定义列表
     * @param nullBitmap NULL标记位图 (用于判断VARCHAR是否为NULL)
     * @return 每列的变长字段长度数组 (非变长字段或NULL字段为 0)
     */
    private static int[] deserializeVariableLengths(ByteBuffer buffer, List<Column> columns,
                                                     boolean[] nullBitmap) {
        int[] lengths = new int[columns.size()];

        // 按列顺序逆序读取
        for (int i = columns.size() - 1; i >= 0; i--) {
            Column col = columns.get(i);
            if (col.getType() != DataType.VARCHAR) {
                continue; // 只处理变长字段
            }

            // ✅ 关键: 根据 NULL bitmap 判断是否为 NULL
            if (nullBitmap[i]) {
                lengths[i] = 0;
                continue; // NULL 的 VARCHAR 不记录长度,跳过读取
            }

            // 非 NULL 的 VARCHAR 读取长度
            byte firstByte = buffer.get();
            int strLen;

            if ((firstByte & 0x80) != 0) {
                // 高位为 1，表示 2 字节长度
                byte secondByte = buffer.get();
                strLen = ((firstByte & 0x7F) << 8) | (secondByte & 0xFF);
            } else {
                // 高位为 0，表示 1 字节长度
                strLen = firstByte & 0xFF;
            }

            lengths[i] = strLen;
        }

        return lengths;
    }

    /**
     * 序列化 NULL 标记位图
     *
     * <p>规则:
     * - bit 1 表示 NULL，bit 0 表示 NOT NULL
     * - 按列顺序排列
     * - 大小 = (列数 + 7) / 8 字节
     *
     * @param row      逻辑行数据
     * @param columns  列定义列表
     * @return NULL 位图 (字节数组)
     */
    private static byte[] serializeNullBitmap(Row row, List<Column> columns) {
        int bitmapSize = (columns.size() + 7) / 8;
        byte[] bitmap = new byte[bitmapSize];

        for (int i = 0; i < columns.size(); i++) {
            if (row.isNull(i)) {
                int byteIndex = i / 8;
                int bitIndex = i % 8;
                bitmap[byteIndex] |= (1 << bitIndex);
            }
        }

        return bitmap;
    }

    /**
     * 反序列化 NULL 标记位图
     *
     * @param buffer   ByteBuffer
     * @param columns  列定义列表
     * @return 每列的 NULL 状态数组
     */
    private static boolean[] deserializeNullBitmap(ByteBuffer buffer, List<Column> columns) {
        int bitmapSize = (columns.size() + 7) / 8;
        byte[] bitmap = new byte[bitmapSize];
        buffer.get(bitmap);

        boolean[] nullFlags = new boolean[columns.size()];
        for (int i = 0; i < columns.size(); i++) {
            int byteIndex = i / 8;
            int bitIndex = i % 8;
            nullFlags[i] = (bitmap[byteIndex] & (1 << bitIndex)) != 0;
        }

        return nullFlags;
    }

    /**
     * 序列化列数据
     *
     * @param row      逻辑行数据
     * @param columns  列定义列表
     * @return 列数据 (字节数组)
     */
    private static byte[] serializeColumnData(Row row, List<Column> columns) {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ByteBuffer buffer;

            for (int i = 0; i < columns.size(); i++) {
                Object value = row.getValue(i);

                // NULL 值跳过
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
                        String str = value.toString();
                        byte[] strBytes = str.getBytes(StandardCharsets.UTF_8);
                        // 重构设计: VARCHAR 不存储长度前缀，长度已在变长字段长度列表中
                        baos.write(strBytes);
                        break;

                    case DATE:
                    case TIMESTAMP:
                        buffer = ByteBuffer.allocate(8);
                        buffer.putLong(((Date) value).getTime());
                        baos.write(buffer.array());
                        break;

                    default:
                        throw new IllegalArgumentException("Unsupported data type: " + type);
                }
            }

            return baos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize column data", e);
        }
    }
}
