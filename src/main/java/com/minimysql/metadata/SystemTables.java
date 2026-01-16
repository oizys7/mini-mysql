package com.minimysql.metadata;

import com.minimysql.storage.table.Column;
import com.minimysql.storage.table.DataType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * SystemTables - 系统表定义
 *
 * 定义元数据管理的系统表结构，对应MySQL的information_schema。
 *
 * 核心设计:
 * 1. SYS_TABLES: 存储所有表的元数据(表ID、表名)
 * 2. SYS_COLUMNS: 存储所有列的元数据(表ID、列名、列类型、长度、可空性)
 *
 * MySQL对应关系:
 * - SYS_TABLES → information_schema.TABLES
 * - SYS_COLUMNS → information_schema.COLUMNS
 *
 * "Good taste"原则:
 * - 系统表就是普通的Table，没有特殊处理
 * - 元数据表在初始化时一次性创建，后续正常使用
 * - 消除"元数据是特殊的"这种特殊情况
 *
 * "实用主义"原则:
 * - 只实现必要的系统表(2个)，不是完整的information_schema
 * - 数据结构简单，易于序列化和反序列化
 * - 不支持复杂的元数据查询，只支持全表扫描
 */
public class SystemTables {

    /**
     * 系统表名称常量
     */
    public static final String SYS_TABLES = "SYS_TABLES";
    public static final String SYS_COLUMNS = "SYS_COLUMNS";

    /**
     * 系统表ID常量(固定值，避免冲突)
     */
    public static final int SYS_TABLES_ID = -1;
    public static final int SYS_COLUMNS_ID = -2;

    /**
     * 获取SYS_TABLES表的列定义
     *
     * 存储格式:
     * - table_id: INT (4字节) - 表ID
     * - table_name: VARCHAR(128) - 表名
     *
     * @return 列定义列表
     */
    public static List<Column> getSysTablesColumns() {
        return new ArrayList<>(Arrays.asList(
                new Column("table_id", DataType.INT, false),
                new Column("table_name", DataType.VARCHAR, 128, false)
        ));
    }

    /**
     * 获取SYS_COLUMNS表的列定义
     *
     * 存储格式:
     * - table_id: INT (4字节) - 表ID
     * - column_name: VARCHAR(128) - 列名
     * - column_type: VARCHAR(32) - 列类型(INT, VARCHAR等)
     * - column_length: INT (4字节) - 类型长度(仅VARCHAR有效)
     * - nullable: BOOLEAN (1字节) - 是否可空
     * - column_position: INT (4字节) - 列在表中的位置(从0开始)
     *
     * @return 列定义列表
     */
    public static List<Column> getSysColumnsColumns() {
        return new ArrayList<>(Arrays.asList(
                new Column("table_id", DataType.INT, false),
                new Column("column_name", DataType.VARCHAR, 128, false),
                new Column("column_type", DataType.VARCHAR, 32, false),
                new Column("column_length", DataType.INT, false),
                new Column("nullable", DataType.BOOLEAN, false),
                new Column("column_position", DataType.INT, false)
        ));
    }

    /**
     * 检查是否为系统表
     *
     * @param tableName 表名
     * @return 如果是系统表返回true
     */
    public static boolean isSystemTable(String tableName) {
        return SYS_TABLES.equals(tableName) || SYS_COLUMNS.equals(tableName);
    }

    /**
     * 检查是否为系统表ID
     *
     * @param tableId 表ID
     * @return 如果是系统表ID返回true
     */
    public static boolean isSystemTableId(int tableId) {
        return tableId == SYS_TABLES_ID || tableId == SYS_COLUMNS_ID;
    }

    /**
     * 获取所有系统表名称
     *
     * @return 系统表名称列表
     */
    public static List<String> getAllSystemTableNames() {
        return new ArrayList<>(Arrays.asList(SYS_TABLES, SYS_COLUMNS));
    }
}
