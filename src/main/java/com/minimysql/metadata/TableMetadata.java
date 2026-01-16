package com.minimysql.metadata;

import com.minimysql.storage.table.Column;
import com.minimysql.storage.table.DataType;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * TableMetadata - 表元数据
 *
 * 在内存中表示一个表的完整元数据定义。
 * 用于SchemaManager和系统表之间的转换。
 *
 * "Good taste": 简单的数据对象，没有复杂的行为
 */
public class TableMetadata {

    /** 表ID */
    private final int tableId;

    /** 表名 */
    private final String tableName;

    /** 列元数据列表 */
    private final List<ColumnMetadata> columns;

    /**
     * 创建表元数据
     *
     * @param tableId 表ID
     * @param tableName 表名
     * @param columns 列元数据列表
     */
    public TableMetadata(int tableId, String tableName, List<ColumnMetadata> columns) {
        this.tableId = tableId;
        this.tableName = tableName;
        this.columns = new ArrayList<>(columns);
    }

    /**
     * 从Table对象创建元数据
     *
     * @param tableId 表ID
     * @param tableName 表名
     * @param columns 列定义列表
     * @return 表元数据
     */
    public static TableMetadata fromColumns(int tableId, String tableName, List<Column> columns) {
        List<ColumnMetadata> columnMetadataList = new ArrayList<>();
        for (int i = 0; i < columns.size(); i++) {
            Column col = columns.get(i);
            columnMetadataList.add(new ColumnMetadata(
                    tableId,
                    col.getName(),
                    col.getType(),
                    col.getLength(),
                    col.isNullable(),
                    i
            ));
        }
        return new TableMetadata(tableId, tableName, columnMetadataList);
    }

    /**
     * 转换为Column列表
     *
     * @return 列定义列表
     */
    public List<Column> toColumns() {
        List<Column> columnList = new ArrayList<>();
        for (ColumnMetadata colMeta : columns) {
            Column col;
            if (colMeta.getType() == DataType.VARCHAR) {
                col = new Column(colMeta.getName(), colMeta.getType(), colMeta.getLength(), colMeta.isNullable());
            } else {
                col = new Column(colMeta.getName(), colMeta.getType(), colMeta.isNullable());
            }
            columnList.add(col);
        }
        return columnList;
    }

    public int getTableId() {
        return tableId;
    }

    public String getTableName() {
        return tableName;
    }

    public List<ColumnMetadata> getColumns() {
        return new ArrayList<>(columns);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        TableMetadata that = (TableMetadata) obj;
        return tableId == that.tableId && Objects.equals(tableName, that.tableName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableId, tableName);
    }

    @Override
    public String toString() {
        return "TableMetadata{" +
                "tableId=" + tableId +
                ", tableName='" + tableName + '\'' +
                ", columns=" + columns.size() +
                '}';
    }
}
