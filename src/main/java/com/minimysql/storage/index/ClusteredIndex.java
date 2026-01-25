package com.minimysql.storage.index;

import com.minimysql.storage.buffer.BufferPool;
import com.minimysql.storage.page.PageManager;
import com.minimysql.storage.table.RecordSerializer;
import com.minimysql.storage.table.Row;
import com.minimysql.storage.table.Table;

import java.util.ArrayList;
import java.util.List;

/**
 * ClusteredIndex - 聚簇索引
 *
 * <p>在 InnoDB 中，聚簇索引就是表本身。
 *
 * <p>MySQL InnoDB 对应关系:
 * <ul>
 *   <li>ClusteredIndex → InnoDB 的表数据组织方式</li>
 *   <li>主键 (Primary Key) → 聚簇索引键</li>
 *   <li>Row (逻辑行) → Logical Row</li>
 *   <li>Record (物理记录) → Physical Record (存储在 B+ 树叶子节点)</li>
 * </ul>
 *
 * <p>核心特性:
 * <ul>
 *   <li>每个表只能有一个聚簇索引 (通常是主键)</li>
 *   <li>数据按主键顺序物理存储</li>
 *   <li>范围查询快 (数据在叶子节点连续存储)</li>
 *   <li>主键查询最快 (直接定位到数据行)</li>
 *   <li>插入性能依赖主键顺序 (顺序插入最优)</li>
 * </ul>
 *
 * <p>与二级索引的区别:
 * <ul>
 *   <li>聚簇索引:叶子节点存储完整行数据 (Physical Record)</li>
 *   <li>二级索引:叶子节点存储主键值 (需要回表查询)</li>
 * </ul>
 *
 * <p>设计原则 (重构后):
 * <ul>
 *   <li>聚簇索引键 = 主键列</li>
 *   <li>B+ 树叶子节点 value = Physical Record (字节数组)</li>
 *   <li>使用 RecordSerializer 进行逻辑 Row 和物理 Record 的转换</li>
 *   <li>支持主键去重 (插入前检查是否存在)</li>
 * </ul>
 *
 * <p>"Good taste": 聚簇索引就是表数据，表就是聚簇索引，两者不可分割
 *
 * <p>参考文档:
 * <ul>
 *   <li>https://dev.mysql.com/doc/refman/8.0/en/innodb-index-types.html</li>
 *   <li>https://dev.mysql.com/doc/refman/8.0/en/clustered-index.html</li>
 * </ul>
 *
 * <p>性能优化建议 (MySQL 兼容):
 * <ul>
 *   <li>主键尽量短 (INT 优于 BIGINT，VARCHAR 尽量短)</li>
 *   <li>主键尽量顺序递增 (AUTO_INCREMENT)</li>
 *   <li>避免随机主键 (如 UUID)，会导致页分裂</li>
 * </ul>
 */
public class ClusteredIndex extends BPlusTree {

    /** 主键列名 */
    private final String primaryKeyColumn;

    /** 主键列在Row中的索引 */
    private final int primaryKeyIndex;

    /** 表引用 (用于获取列定义和序列化/反序列化) */
    private Table table;

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
     * 设置表引用
     *
     * @param table 表对象
     */
    public void setTable(Table table) {
        this.table = table;
    }

    /**
     * 获取表引用
     *
     * @return 表对象
     */
    public Table getTable() {
        return table;
    }

    /**
     * 插入行到聚簇索引
     *
     * <p>流程:
     * <ol>
     *   <li>提取主键值</li>
     *   <li>使用 RecordSerializer 将逻辑 Row 序列化为物理 Record</li>
     *   <li>插入到 B+ 树: key=主键, value=物理记录</li>
     * </ol>
     *
     * @param row 逻辑行数据
     * @throws IllegalArgumentException 如果主键为 NULL
     * @throws IllegalStateException  如果 Table 未设置
     */
    public void insertRow(Row row) {
        if (table == null) {
            throw new IllegalStateException("Table not set for ClusteredIndex");
        }

        Object primaryKeyValue = row.getValue(primaryKeyIndex);
        if (primaryKeyValue == null) {
            throw new IllegalArgumentException("Primary key cannot be NULL");
        }

        // Logical Row → Physical Record
        byte[] physicalRecord = RecordSerializer.serialize(row, table.getColumns());

        // 插入到 B+ 树
        int key = hashKey(primaryKeyValue);
        insert(key, physicalRecord);
    }

    /**
     * 根据主键查询行
     *
     * <p>流程:
     * <ol>
     *   <li>从 B+ 树查询物理记录</li>
     *   <li>使用 RecordSerializer 将物理 Record 反序列化为逻辑 Row</li>
     * </ol>
     *
     * @param primaryKeyValue 主键值
     * @return 逻辑行数据，不存在返回 null
     * @throws IllegalStateException 如果 Table 未设置
     */
    public Row selectByPrimaryKey(Object primaryKeyValue) {
        if (table == null) {
            throw new IllegalStateException("Table not set for ClusteredIndex");
        }

        int key = hashKey(primaryKeyValue);
        Object value = search(key);

        if (value != null) {
            byte[] physicalRecord = (byte[]) value;
            // Physical Record → Logical Row
            return RecordSerializer.deserialize(physicalRecord, table.getColumns());
        }
        return null;
    }

    /**
     * 主键范围查询
     *
     * <p>利用 B+ 树叶子节点链表，高效支持范围查询。
     *
     * <p>流程:
     * <ol>
     *   <li>在 B+ 树中范围查询 [startKey, endKey]</li>
     *   <li>将所有物理记录反序列化为逻辑 Row</li>
     * </ol>
     *
     * @param startValue 起始主键值 (包含)
     * @param endValue   结束主键值 (包含)
     * @return 逻辑行列表
     * @throws IllegalStateException 如果 Table 未设置
     */
    public List<Row> rangeSelect(Object startValue, Object endValue) {
        if (table == null) {
            throw new IllegalStateException("Table not set for ClusteredIndex");
        }

        int startKey = hashKey(startValue);
        int endKey = hashKey(endValue);

        List<Object> physicalRecords = rangeSearch(startKey, endKey);
        List<Row> rows = new ArrayList<>();

        for (Object record : physicalRecords) {
            byte[] physicalRecord = (byte[]) record;
            // Physical Record → Logical Row
            rows.add(RecordSerializer.deserialize(physicalRecord, table.getColumns()));
        }

        return rows;
    }

    /**
     * 检查主键是否存在
     *
     * <p>用于主键唯一性约束。
     *
     * @param primaryKeyValue 主键值
     * @return 存在返回 true
     */
    public boolean exists(Object primaryKeyValue) {
        return selectByPrimaryKey(primaryKeyValue) != null;
    }

    /**
     * 获取所有行 (全表扫描)
     *
     * <p>遍历聚簇索引的所有叶子节点，返回所有行数据。
     * 时间复杂度 O(N)，性能较差，应尽量避免使用。
     *
     * <p>MySQL InnoDB 对应:
     * <ul>
     *   <li>全表扫描 → 扫描聚簇索引的所有叶子节点</li>
     *   <li>顺序 I/O → 叶子节点通过链表连接，顺序读取效率高</li>
     * </ul>
     *
     * @return 所有逻辑行数据
     * @throws IllegalStateException 如果 Table 未设置
     */
    public List<Row> getAllRows() {
        if (table == null) {
            throw new IllegalStateException("Table not set for ClusteredIndex");
        }

        List<Object> physicalRecords = getAll();
        List<Row> rows = new ArrayList<>();

        for (Object record : physicalRecords) {
            byte[] physicalRecord = (byte[]) record;
            // Physical Record → Logical Row
            rows.add(RecordSerializer.deserialize(physicalRecord, table.getColumns()));
        }

        return rows;
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
