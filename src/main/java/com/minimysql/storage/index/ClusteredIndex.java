package com.minimysql.storage.index;

import com.minimysql.storage.buffer.BufferPool;
import com.minimysql.storage.page.PageManager;
import com.minimysql.storage.table.Column;
import com.minimysql.storage.table.Row;

import java.util.List;

/**
 * ClusteredIndex - 聚簇索引
 *
 * 聚簇索引是主键索引,数据和索引存储在一起。
 * InnoDB中,表数据就是按照聚簇索引组织的。
 *
 * MySQL InnoDB对应关系:
 * - ClusteredIndex → InnoDB的表数据组织方式
 * - 主键 → 聚簇索引键
 * - Row → 实际表行数据
 * - 叶子节点value → Row对象(完整行数据)
 *
 * 核心特性:
 * - 每个表只能有一个聚簇索引(通常是主键)
 * - 数据按主键顺序物理存储
 * - 范围查询快(数据在叶子节点连续存储)
 * - 主键查询最快(直接定位到数据行)
 * - 插入性能依赖主键顺序(顺序插入最优)
 *
 * 与二级索引的区别:
 * - 聚簇索引:叶子节点存储完整Row数据
 * - 二级索引:叶子节点存储主键值(需要回表查询)
 *
 * 设计原则:
 * - 聚簇索引键 = 主键列
 * - B+树叶子节点value = Row序列化数据
 * - 支持主键去重(插入前检查是否存在)
 *
 * "Good taste": 聚簇索引就是表数据,表就是聚簇索引,两者不可分割
 *
 * 性能优化建议(MySQL兼容):
 * - 主键尽量短(INT优于BIGINT,VARCHAR尽量短)
 * - 主键尽量顺序递增(AUTO_INCREMENT)
 * - 避免随机主键(如UUID),会导致页分裂
 */
public class ClusteredIndex extends BPlusTree {

    /** 主键列名 */
    private final String primaryKeyColumn;

    /** 主键列在Row中的索引 */
    private final int primaryKeyIndex;

    /** 列定义(用于Row序列化和反序列化) */
    private List<Column> columns;

    /**
     * 创建聚簇索引
     *
     * @param tableId 表ID
     * @param primaryKeyColumn 主键列名
     * @param primaryKeyIndex 主键列索引
     * @param bufferPool 全局BufferPool
     * @param pageManager 索引页管理器
     */
    public ClusteredIndex(int tableId, String primaryKeyColumn, int primaryKeyIndex,
                          BufferPool bufferPool, PageManager pageManager) {
        // 聚簇索引的indexId = tableId * 100 (保留空间给二级索引)
        super(tableId * 100, "PRIMARY", true, primaryKeyColumn, bufferPool, pageManager);

        this.primaryKeyColumn = primaryKeyColumn;
        this.primaryKeyIndex = primaryKeyIndex;
    }

    /**
     * 重写根节点创建方法,设置valueType为BYTES
     */
    @Override
    protected BPlusTreeNode createRootNode() {
        BPlusTreeNode root = new BPlusTreeNode(true);
        root.setValueType(BPlusTreeNode.VALUE_TYPE_BYTES);
        return root;
    }

    /**
     * 设置列定义
     *
     * @param columns 列定义列表
     */
    public void setColumns(List<Column> columns) {
        this.columns = columns;
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
     * 插入行到聚簇索引
     *
     * @param row 行数据
     */
    public void insertRow(Row row) {
        Object primaryKeyValue = row.getValue(primaryKeyIndex);
        if (primaryKeyValue == null) {
            throw new IllegalArgumentException("Primary key cannot be NULL");
        }

        int key = hashKey(primaryKeyValue);
        byte[] rowBytes = row.toBytes();
        insert(key, rowBytes);
    }

    /**
     * 根据主键查询行
     *
     * @param primaryKeyValue 主键值
     * @return 行数据,不存在返回null
     */
    public Row selectByPrimaryKey(Object primaryKeyValue) {
        int key = hashKey(primaryKeyValue);
        Object value = search(key);

        if (value != null) {
            byte[] rowBytes = (byte[]) value;
            return Row.fromBytes(columns, rowBytes);
        }
        return null;
    }

    /**
     * 主键范围查询
     *
     * 利用B+树叶子节点链表,高效支持范围查询。
     *
     * @param startValue 起始主键值(包含)
     * @param endValue 结束主键值(包含)
     * @return 行列表
     */
    public java.util.List<Row> rangeSelect(Object startValue, Object endValue) {
        int startKey = hashKey(startValue);
        int endKey = hashKey(endValue);

        java.util.List<Object> values = rangeSearch(startKey, endKey);
        java.util.List<Row> rows = new java.util.ArrayList<>();

        for (Object value : values) {
            // value是byte[],需要反序列化为Row
            byte[] rowBytes = (byte[]) value;
            rows.add(Row.fromBytes(columns, rowBytes));
        }

        return rows;
    }

    /**
     * 检查主键是否存在
     *
     * 用于主键唯一性约束。
     *
     * @param primaryKeyValue 主键值
     * @return 存在返回true
     */
    public boolean exists(Object primaryKeyValue) {
        return selectByPrimaryKey(primaryKeyValue) != null;
    }

    /**
     * 将主键值哈希为int
     *
     * 简化实现:
     * - INT类型:直接返回
     * - VARCHAR类型:使用hashCode
     * - 其他类型:暂不支持
     *
     * 生产环境应该:
     * - INT:直接使用
     * - BIGINT:高位与低位组合
     * - VARCHAR:前缀+哈希(避免哈希冲突)
     *
     * @param keyValue 主键值
     * @return int类型的键
     */
    private int hashKey(Object keyValue) {
        if (keyValue instanceof Integer) {
            return (Integer) keyValue;
        } else if (keyValue instanceof String) {
            // 简化:使用hashCode(实际应该处理哈希冲突)
            return keyValue.hashCode();
        } else if (keyValue instanceof Long) {
            // BIGINT:取低位(简化)
            return ((Long) keyValue).hashCode();
        } else {
            throw new IllegalArgumentException(
                    "Unsupported primary key type: " + keyValue.getClass().getSimpleName());
        }
    }

    /**
     * 获取主键列名
     */
    public String getPrimaryKeyColumn() {
        return primaryKeyColumn;
    }

    /**
     * 获取主键列索引
     */
    public int getPrimaryKeyIndex() {
        return primaryKeyIndex;
    }

    @Override
    public String toString() {
        return "ClusteredIndex{" +
                "tableId=" + (getIndexId() / 100) +
                ", primaryKeyColumn='" + primaryKeyColumn + '\'' +
                ", primaryKeyIndex=" + primaryKeyIndex +
                ", height=" + getHeight() +
                '}';
    }
}
