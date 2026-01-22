package com.minimysql.storage.index;

import com.minimysql.storage.buffer.BufferPool;
import com.minimysql.storage.page.PageManager;
import com.minimysql.storage.table.Row;

/**
 * SecondaryIndex - 二级索引
 *
 * 二级索引是辅助索引,建立在非主键列上。
 * InnoDB中,二级索引的叶子节点存储主键值,而不是完整行数据。
 *
 * MySQL InnoDB对应关系:
 * - SecondaryIndex → InnoDB的二级索引
 * - 索引列键 → B+树键
 * - 叶子节点value → 主键值(需要回表查询)
 * - 回表查询 → 通过主键值在聚簇索引中查找完整行
 *
 * 核心特性:
 * - 一个表可以有多个二级索引
 * - 二级索引叶子节点存储主键值,不是完整行数据
 * - 查询流程:二级索引找主键 → 聚簇索引找完整行(两次B+树查找)
 * - 覆盖索引可以避免回表(如果查询列都在索引中)
 *
 * 与聚簇索引的区别:
 * - 聚簇索引:叶子节点存储完整Row → 直接返回数据
 * - 二级索引:叶子节点存储主键值 → 需要回表查询
 *
 * 设计原则:
 * - 二级索引键 = 索引列值
 * - B+树叶子节点value = 主键值(Integer)
 * - 支持索引列去重(如果UNIQUE索引)
 * - 回表查询通过ClusteredIndex完成
 *
 * 回表查询示例:
 * ```sql
 * SELECT * FROM users WHERE username = 'Alice';
 * ```
 * 执行流程:
 * 1. 在username二级索引中查找 'Alice' → 找到主键id=5
 * 2. 在聚簇索引中查找id=5 → 找到完整Row数据
 *
 * 覆盖索引示例(无需回表):
 * ```sql
 * SELECT id FROM users WHERE username = 'Alice';
 * ```
 * 因为id是主键,已经在二级索引中,无需回表。
 *
 * "Good taste": 二级索引和聚簇索引结构完全一致,只是叶子节点value不同
 *
 * 性能权衡(MySQL兼容):
 * - 二级索引加速查询,但降低插入/删除性能(需要更新多个索引)
 * - 索引不是越多越好(每个索引占用存储,影响写入性能)
 * - 高选择性列(唯一值多)适合建索引
 * - 低选择性列(如性别)不适合建索引
 */
public class SecondaryIndex extends BPlusTree {

    /** 主键列在Row中的索引(用于回表) */
    private final int primaryKeyIndex;

    /** 是否为唯一索引 */
    private final boolean unique;

    /** 聚簇索引引用(用于回表查询) */
    private final ClusteredIndex clusteredIndex;

    /**
     * 创建二级索引
     *
     * @param tableId 表ID
     * @param indexName 索引名称
     * @param columnName 索引列名
     * @param primaryKeyIndex 主键列索引
     * @param unique 是否为唯一索引
     * @param clusteredIndex 聚簇索引(用于回表)
     * @param bufferPool 全局BufferPool
     * @param pageManager 索引页管理器
     */
    public SecondaryIndex(int tableId, String indexName, String columnName,
                          int primaryKeyIndex, boolean unique,
                          ClusteredIndex clusteredIndex,
                          BufferPool bufferPool, PageManager pageManager) {
        // 二级索引的indexId = tableId * 100 + 索引编号(1, 2, 3, ...)
        super(tableId * 100 + 1, indexName, false, columnName, bufferPool, pageManager);

        this.primaryKeyIndex = primaryKeyIndex;
        this.unique = unique;
        this.clusteredIndex = clusteredIndex;
    }

    /**
     * 插入索引条目
     *
     * 将索引列值和主键值插入到二级索引。
     *
     * @param indexColumnValue 索引列值
     * @param primaryKeyValue 主键值
     */
    public void insertEntry(Object indexColumnValue, Object primaryKeyValue) {
        if (primaryKeyValue == null) {
            throw new IllegalArgumentException("Primary key cannot be NULL");
        }

        if (indexColumnValue == null) {
            // NULL值不索引(MySQL兼容)
            return;
        }

        // 唯一索引检查
        if (unique && exists(indexColumnValue)) {
            throw new IllegalArgumentException(
                    "Duplicate entry '" + indexColumnValue + "' for key '" + getIndexName() + "'");
        }

        // 将索引列值哈希为键
        int key = hashKey(indexColumnValue);

        // 将主键值哈希为值(简化:只支持INT主键)
        int value = hashPrimaryKey(primaryKeyValue);

        // 插入到B+树(调用int版本避免类型转换)
        insertInt(key, value);
    }

    /**
     * 根据索引列值查找主键
     *
     * @param indexColumnValue 索引列值
     * @return 主键值,不存在返回null
     */
    public Object findPrimaryKey(Object indexColumnValue) {
        if (indexColumnValue == null) {
            return null; // NULL值不在索引中
        }

        int key = hashKey(indexColumnValue);
        Object value = searchInt(key);

        if (value != null) {
            return value;
        }

        return null;
    }

    /**
     * 根据索引列值查找完整行(回表查询)
     *
     * @param indexColumnValue 索引列值
     * @return 完整行数据,不存在返回null
     */
    public Row selectRow(Object indexColumnValue) {
        Object primaryKeyValue = findPrimaryKey(indexColumnValue);

        if (primaryKeyValue != null) {
            // 回表查询:在聚簇索引中查找完整行
            return clusteredIndex.selectByPrimaryKey(primaryKeyValue);
        }

        return null;
    }

    /**
     * 索引列范围查询
     *
     * @param startValue 起始值(包含)
     * @param endValue 结束值(包含)
     * @return 主键值列表
     */
    public java.util.List<Object> rangeSelect(Object startValue, Object endValue) {
        int startKey = hashKey(startValue);
        int endKey = hashKey(endValue);

        return rangeSearchInt(startKey, endKey);
    }

    /**
     * 检查索引列值是否存在
     *
     * 用于唯一索引约束。
     *
     * @param indexColumnValue 索引列值
     * @return 存在返回true
     */
    public boolean exists(Object indexColumnValue) {
        return findPrimaryKey(indexColumnValue) != null;
    }

    /**
     * 将索引列值哈希为int
     *
     * @param indexValue 索引列值
     * @return int类型的键
     */
    private int hashKey(Object indexValue) {
        if (indexValue instanceof Integer) {
            return (Integer) indexValue;
        } else if (indexValue instanceof String) {
            return indexValue.hashCode();
        } else if (indexValue instanceof Long) {
            return ((Long) indexValue).hashCode();
        } else {
            throw new IllegalArgumentException(
                    "Unsupported index column type: " + indexValue.getClass().getSimpleName());
        }
    }

    /**
     * 将主键值哈希为int
     *
     * 简化实现:只支持INT主键
     *
     * @param primaryKeyValue 主键值
     * @return int类型的主键
     */
    private int hashPrimaryKey(Object primaryKeyValue) {
        if (primaryKeyValue instanceof Integer) {
            return (Integer) primaryKeyValue;
        } else {
            throw new IllegalArgumentException(
                    "Unsupported primary key type: " + primaryKeyValue.getClass().getSimpleName());
        }
    }

    /**
     * 是否为唯一索引
     */
    public boolean isUnique() {
        return unique;
    }

    /**
     * 获取主键列索引
     */
    public int getPrimaryKeyIndex() {
        return primaryKeyIndex;
    }

    /**
     * 获取聚簇索引
     */
    public ClusteredIndex getClusteredIndex() {
        return clusteredIndex;
    }

    @Override
    public String toString() {
        return "SecondaryIndex{" +
                "tableId=" + (getIndexId() / 100) +
                ", indexName='" + getIndexName() + '\'' +
                ", columnName='" + getColumnName() + '\'' +
                ", unique=" + unique +
                ", height=" + getHeight() +
                '}';
    }
}
