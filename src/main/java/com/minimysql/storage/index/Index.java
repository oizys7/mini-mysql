package com.minimysql.storage.index;

import java.util.List;

/**
 * Index - 索引接口
 *
 * 定义所有索引类型的通用操作，采用策略模式支持多种索引实现。
 *
 * 核心功能:
 * 1. 数据操作: 插入、查询、删除
 * 2. 范围查询: 利用B+树叶子节点链表
 * 3. 元数据查询: 索引名称、列名、类型
 *
 * 设计原则:
 * - 接口隔离: 只定义索引必需的操作
 * - 引擎无关: 上层不依赖具体索引实现
 * - 可扩展: 新增索引类型只需实现此接口
 *
 * MySQL InnoDB对应关系:
 * - Index接口 → MySQL索引抽象层
 * - ClusteredIndex → InnoDB聚簇索引
 * - SecondaryIndex → InnoDB二级索引
 * - (预留) HashIndex → Memory引擎的哈希索引
 * - (预留) FullTextIndex → 全文索引
 *
 * 使用模式:
 * <pre>
 * // 使用接口类型引用
 * Index index = new ClusteredIndex(...);
 *
 * // 插入索引条目
 * index.insert(key, value);
 *
 * // 查询
 * Object value = index.search(key);
 *
 * // 范围查询
 * List&lt;Object&gt; results = index.rangeSearch(startKey, endKey);
 * </pre>
 *
 * "Good taste": 所有索引统一接口，消除重复代码
 *
 * "实用主义": 只定义核心方法，不过度抽象
 */
public interface Index {

    /**
     * 获取索引名称
     *
     * @return 索引名称
     */
    String getIndexName();

    /**
     * 获取索引列名
     *
     * @return 索引列名
     */
    String getColumnName();

    /**
     * 是否为聚簇索引
     *
     * 聚簇索引特点:
     * - 每个表只能有一个聚簇索引（通常是主键）
     * - 数据按聚簇索引键的顺序物理存储
     * - 叶子节点存储完整行数据
     *
     * @return 如果是聚簇索引返回true
     */
    boolean isClustered();

    /**
     * 是否为唯一索引
     *
     * 唯一索引特点:
     * - 索引列的值必须唯一
     * - 插入重复值会抛出异常
     * - 支持快速查找（无需处理重复键）
     *
     * @return 如果是唯一索引返回true
     */
    boolean isUnique();

    /**
     * 插入索引条目
     *
     * 将键值对插入到索引中。
     * 如果是唯一索引且键已存在，抛出异常。
     *
     * @param key 索引键
     * @param value 索引值
     * @throws IllegalArgumentException 如果是唯一索引且键已存在
     */
    void insert(Object key, Object value);

    /**
     * 查找键
     *
     * 根据键查找对应的值。
     * 时间复杂度: O(log N)
     *
     * @param key 索引键
     * @return 对应的值，如果不存在返回null
     */
    Object search(Object key);

    /**
     * 范围查询
     *
     * 查找键在指定范围内的所有值。
     * 对于B+树索引，利用叶子节点链表高效实现。
     *
     * @param startKey 起始键（包含）
     * @param endKey 结束键（包含）
     * @return 值列表
     */
    List<Object> rangeSearch(Object startKey, Object endKey);

    /**
     * 删除索引条目
     *
     * 从索引中删除指定的键。
     * B+树删除需要处理节点合并和借位。
     *
     * @param key 索引键
     * @throws UnsupportedOperationException 如果索引类型不支持删除
     */
    void delete(Object key);

    /**
     * 检查键是否存在
     *
     * @param key 索引键
     * @return 如果键存在返回true
     */
    default boolean exists(Object key) {
        return search(key) != null;
    }

    /**
     * 获取索引高度
     *
     * 对于树形索引（如B+树），返回树的高度。
     * 高度=1表示只有根节点。
     *
     * @return 索引高度，如果不适用返回-1
     */
    int getHeight();

    /**
     * 获取索引ID
     *
     * 索引ID用于标识不同的索引实例。
     * 格式: tableId * 100 + indexNumber
     *
     * @return 索引ID
     */
    int getIndexId();
}
