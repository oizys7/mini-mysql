package com.minimysql.storage.table;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Date;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * RecordSerializer 单元测试
 *
 * 测试InnoDB COMPACT行格式的序列化和反序列化功能
 */
@DisplayName("RecordSerializer - InnoDB行格式序列化测试")
class RecordSerializerTest {

    @Test
    @DisplayName("序列化和反序列化简单VARCHAR和INT类型")
    void testSimpleVarcharSerialization() {
        List<Column> columns = Arrays.asList(
                new Column("id", DataType.INT, false),
                new Column("name", DataType.VARCHAR, 100, true)
        );

        Object[] values = {100, "Test User"};
        Row row = new Row(values);

        byte[] data = RecordSerializer.serialize(row, columns);

        Row restored = RecordSerializer.deserialize(data, columns);

        assertEquals(100, restored.getInt(0));
        assertEquals("Test User", restored.getString(1));
    }

    @Test
    @DisplayName("序列化和反序列化多种数据类型(INT, VARCHAR, DOUBLE, BOOLEAN, TIMESTAMP)")
    void testMultipleTypes() {
        List<Column> columns = Arrays.asList(
                new Column("id", DataType.INT, false),
                new Column("name", DataType.VARCHAR, 100, true),
                new Column("age", DataType.INT, false),
                new Column("balance", DataType.DOUBLE, true),
                new Column("active", DataType.BOOLEAN, false),
                new Column("created_at", DataType.TIMESTAMP, true)
        );

        Date now = new Date();
        Object[] values = {
                100,
                "Test User",
                35,
                9999.99,
                false,
                now
        };

        Row row = new Row(values);

        byte[] data = RecordSerializer.serialize(row, columns);

        Row restored = RecordSerializer.deserialize(data, columns);

        assertEquals(100, restored.getInt(0));
        assertEquals("Test User", restored.getString(1));
        assertEquals(35, restored.getInt(2));
        assertEquals(9999.99, restored.getDouble(3), 0.001);
        assertEquals(false, restored.getBoolean(4));
        assertEquals(now.getTime(), restored.getDate(5).getTime());
    }

    @Test
    @DisplayName("正确处理NULL值(NULL bitmap标记且不占数据空间)")
    void testNullValues() {
        List<Column> columns = Arrays.asList(
                new Column("id", DataType.INT, false),
                new Column("name", DataType.VARCHAR, 100, true),
                new Column("age", DataType.INT, false)
        );

        Object[] values = {1, null, 25};
        Row row = new Row(values);

        byte[] data = RecordSerializer.serialize(row, columns);
        Row restored = RecordSerializer.deserialize(data, columns);

        assertEquals(1, restored.getInt(0));
        assertNull(restored.getValue(1));
        assertEquals(25, restored.getInt(2));
    }

    @Test
    @DisplayName("验证VARCHAR和INT的序列化/反序列化对称性")
    void testSymmetryVarcharInt() {
        List<Column> columns = Arrays.asList(
                new Column("id", DataType.INT, false),
                new Column("name", DataType.VARCHAR, 100, true)
        );

        Object[] originalValues = {100, "Test User"};
        Row originalRow = new Row(originalValues);

        // 序列化
        byte[] data = RecordSerializer.serialize(originalRow, columns);

        // 反序列化
        Row restoredRow = RecordSerializer.deserialize(data, columns);

        // 验证对称性
        assertEquals(originalRow.getInt(0), restoredRow.getInt(0), "ID should match");
        assertEquals(originalRow.getString(1), restoredRow.getString(1), "Name should match");
    }

    @Test
    @DisplayName("验证所有数据类型的序列化/反序列化对称性")
    void testSymmetryAllDataTypes() {
        List<Column> columns = Arrays.asList(
                new Column("col_int", DataType.INT, false),
                new Column("col_varchar", DataType.VARCHAR, 50, true),
                new Column("col_double", DataType.DOUBLE, false),
                new Column("col_boolean", DataType.BOOLEAN, false)
        );

        Object[] originalValues = {42, "Hello", 3.14, true};
        Row originalRow = new Row(originalValues);

        byte[] data = RecordSerializer.serialize(originalRow, columns);
        Row restoredRow = RecordSerializer.deserialize(data, columns);

        assertEquals(originalRow.getInt(0), restoredRow.getInt(0));
        assertEquals(originalRow.getString(1), restoredRow.getString(1));
        assertEquals(originalRow.getDouble(2), restoredRow.getDouble(2), 0.001);
        assertEquals(originalRow.getBoolean(3), restoredRow.getBoolean(3));
    }
}
