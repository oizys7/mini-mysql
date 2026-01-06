package com.minimysql.storage.index;

import com.minimysql.storage.buffer.BufferPool;
import com.minimysql.storage.buffer.PageFrame;
import com.minimysql.storage.page.DataPage;
import com.minimysql.storage.page.PageManager;

import java.util.ArrayList;
import java.util.List;

/**
 * BPlusTree - B+树索引
 *
 * B+树是最常用的数据库索引结构,MySQL InnoDB、PostgreSQL等都使用B+树。
 *
 * 核心特性(MySQL兼容):
 * - 所有数据在叶子节点,内部节点只存索引
 * - 叶子节点形成有序链表(支持范围查询)
 * - 查询效率稳定(O(log N),总是从根到叶)
 * - 节点大小=页大小(16KB),对应InnoDB索引页
 * - 支持聚簇索引(主键)和二级索引
 *
 * 设计原则:
 * - 节点存储在DataPage中,通过BufferPool管理
 * - 每个索引有独立的PageManager
 * - pageId=0保留给根节点
 * - 树的高度自动调整(插入分裂,删除合并)
 *
 * B+树 vs B树:
 * - B+树所有数据在叶子节点 → 范围查询快
 * - B树数据在所有节点 → 单次查询可能更快
 * - 数据库选择B+树是因为范围查询更常见
 *
 * "Good taste": 统一的节点结构,内部节点和叶子节点差异最小化
 *
 * 持久化实现:
 * - 节点序列化到DataPage
 * - 通过BufferPool管理页缓存
 * - 脏页标记和刷盘
 *
 * todo 1. B+树递归插入未完整实现: 需要处理节点分裂时的valueType传递
 *      2. 范围查询未完整实现: 需要遍历叶子节点链表
 *      3. 删除操作未实现: B+树删除需要节点合并和借位
 */
public class BPlusTree {

    /** 索引ID(对应表ID+索引编号) */
    private final int indexId;

    /** 索引名称 */
    private final String indexName;

    /** 是否为聚簇索引(主键索引) */
    private final boolean isClustered;

    /** 索引列(简化:只支持单列索引) */
    private final String columnName;

    /** 全局共享的BufferPool */
    private final BufferPool bufferPool;

    /** 索引页管理器(每个索引独立) */
    private final PageManager pageManager;

    /** 根节点pageId(固定为0) */
    private static final int ROOT_PAGE_ID = 0;

    /** 树高度 */
    private int height;

    /**
     * 创建B+树索引
     *
     * @param indexId 索引ID
     * @param indexName 索引名称
     * @param isClustered 是否为聚簇索引
     * @param columnName 索引列名
     * @param bufferPool 全局BufferPool
     * @param pageManager 索引页管理器
     */
    public BPlusTree(int indexId, String indexName, boolean isClustered,
                     String columnName, BufferPool bufferPool, PageManager pageManager) {
        this.indexId = indexId;
        this.indexName = indexName;
        this.isClustered = isClustered;
        this.columnName = columnName;
        this.bufferPool = bufferPool;
        this.pageManager = pageManager;

        // 加载或创建索引
        loadOrCreate();
    }

    /**
     * 加载或创建索引
     *
     * 如果索引已存在,加载根节点;
     * 否则创建新的根节点(叶子节点)。
     */
    private void loadOrCreate() {
        pageManager.load(indexId);

        if (pageManager.getAllocatedPageCount() == 0) {
            // 创建新索引:分配根节点(叶子节点)
            BPlusTreeNode root = createRootNode();
            root.setPageId(ROOT_PAGE_ID);

            // 持久化根节点
            saveNode(root);

            this.height = 1;
        } else {
            // 加载现有索引:从页0读取根节点
            // ⚠️ 简化实现:只加载根节点,不恢复完整的树结构
            // 生产环境:需要递归加载所有节点,恢复height
            BPlusTreeNode root = loadNode(ROOT_PAGE_ID);

            // TODO: 从持久化存储加载树高度
            this.height = 1; // 简化:默认高度为1
        }
    }

    /**
     * 创建根节点(子类可重写)
     *
     * @return 根节点
     */
    protected BPlusTreeNode createRootNode() {
        return new BPlusTreeNode(true);
    }

    /**
     * 查找键
     *
     * 从根节点开始,递归查找键。
     *
     * @param key 键值(INT类型的哈希码)
     * @return 找到的值,不存在返回null
     */
    public Object search(int key) {
        BPlusTreeNode root = loadNode(ROOT_PAGE_ID);
        return search(root, key);
    }

    /**
     * 递归查找键
     *
     * @param node 当前节点
     * @param key 键值
     * @return 找到的值,不存在返回null
     */
    private Object search(BPlusTreeNode node, int key) {
        if (node.isLeaf()) {
            // 叶子节点:在键数组中查找
            int pos = node.findKeyPosition(key);

            if (pos < node.getKeyCount() && node.getKey(pos) == key) {
                return node.getValue(pos);
            }

            return null; // 未找到
        } else {
            // 内部节点:找到子节点,递归查找
            int pos = node.findKeyPosition(key);
            int childPageId = node.getChild(pos);
            BPlusTreeNode child = loadNode(childPageId);
            return search(child, key);
        }
    }

    /**
     * 分裂叶子节点
     *
     * @param node 叶子节点
     */
    private void splitLeafNode(BPlusTreeNode node) {
        BPlusTreeNode.SplitResult splitResult = node.split();

        // 如果是根节点(pageId=0),需要创建新根
        if (node.getPageId() == ROOT_PAGE_ID) {
            BPlusTreeNode newRoot = new BPlusTreeNode(false);
            newRoot.setPageId(ROOT_PAGE_ID);

            // 新根的第一个子节点指向原根
            newRoot.setChild(0, node.getPageId());

            // 插入分裂键和新节点
            int newChildPageId = pageManager.allocatePage();
            splitResult.newNode.setPageId(newChildPageId);
            saveNode(splitResult.newNode);

            newRoot.insertChild(splitResult.splitKey, newChildPageId);

            // 更新树高度
            height++;
            saveNode(newRoot);
        } else {
            // 非根节点:只需保存分裂后的节点
            int newChildPageId = pageManager.allocatePage();
            splitResult.newNode.setPageId(newChildPageId);
            saveNode(splitResult.newNode);
        }
    }

    /**
     * 插入键值对
     *
     * @param key 键值
     * @param value 值
     */
    public void insert(int key, Object value) {
        BPlusTreeNode root = loadNode(ROOT_PAGE_ID);
        insert(root, key, value);
    }

    /**
     * 递归插入键值对
     *
     * @param node 当前节点
     * @param key 键值
     * @param value 值
     */
    private void insert(BPlusTreeNode node, int key, Object value) {
        if (node.isLeaf()) {
            // 叶子节点:直接插入
            node.insertKeyValue(key, value);
            saveNode(node);

            // 检查是否需要分裂
            if (node.needsSplit()) {
                splitLeafNode(node);
            }
        } else {
            // 内部节点:找到子节点,递归插入
            int pos = node.findKeyPosition(key);
            int childPageId = node.getChild(pos);
            BPlusTreeNode child = loadNode(childPageId);

            // 检查子节点是否需要分裂
            if (child.needsSplit()) {
                // 分离子节点
                BPlusTreeNode.SplitResult splitResult = child.split();

                // 为新节点分配pageId
                int newChildPageId = pageManager.allocatePage();
                splitResult.newNode.setPageId(newChildPageId);
                saveNode(splitResult.newNode);

                // 将分裂键和新子节点插入当前节点
                node.insertChild(splitResult.splitKey, newChildPageId);

                // 决定继续插入哪个子节点
                if (key <= splitResult.splitKey) {
                    insert(child, key, value);
                } else {
                    insert(splitResult.newNode, key, value);
                }

                saveNode(node);
            } else {
                // 子节点不需要分裂,继续插入
                insert(child, key, value);
            }
        }
    }

    /**
     * 找到包含键的叶子节点
     *
     * @param node 起始节点
     * @param key 键值
     * @return 叶子节点
     */
    private BPlusTreeNode findLeafNode(BPlusTreeNode node, int key) {
        if (node.isLeaf()) {
            return node;
        } else {
            // 内部节点:找到子节点,递归查找
            int pos = node.findKeyPosition(key);
            int childPageId = node.getChild(pos);
            BPlusTreeNode child = loadNode(childPageId);
            return findLeafNode(child, key);
        }
    }

    /**
     * 删除键(简化实现:暂不实现)
     *
     * B+树删除很复杂,需要处理节点合并、借位等。
     * 生产环境必须实现,学习项目可以先跳过。
     *
     * @param key 键值
     */
    public void delete(int key) {
        throw new UnsupportedOperationException("Delete not implemented yet");
    }

    /**
     * 范围查询
     *
     * 利用叶子节点链表,支持范围查询。
     * 这是B+树相比B树的核心优势。
     *
     * @param startKey 起始键(包含)
     * @param endKey 结束键(包含)
     * @return 键值对列表
     */
    public List<Object> rangeSearch(int startKey, int endKey) {
        List<Object> results = new ArrayList<>();

        // 1. 找到起始叶子节点
        BPlusTreeNode root = loadNode(ROOT_PAGE_ID);
        BPlusTreeNode leaf = findLeafNode(root, startKey);

        // 2. 在叶子链表中遍历
        while (leaf != null) {
            for (int i = 0; i < leaf.getKeyCount(); i++) {
                int key = leaf.getKey(i);

                if (key > endKey) {
                    return results; // 超出范围,结束
                }

                if (key >= startKey) {
                    results.add(leaf.getValue(i));
                }
            }

            // 移动到下一个叶子节点
            int nextLeafPageId = leaf.getNextLeafPageId();
            if (nextLeafPageId == -1) {
                break;
            }

            leaf = loadNode(nextLeafPageId);
        }

        return results;
    }

    /**
     * 从BufferPool加载节点
     *
     * @param pageId 页号
     * @return 节点对象
     */
    private BPlusTreeNode loadNode(int pageId) {
        PageFrame frame = bufferPool.getPage(indexId, pageId);
        frame.pin();

        try {
            DataPage page = (DataPage) frame.getPage();
            byte[] pageData = page.getData();

            // 检查页是否已初始化(通过Magic Number判断)
            if (pageData.length < 4) {
                // 页未初始化,返回空节点
                return new BPlusTreeNode(true);
            }

            // 读取Magic Number判断是否为B+树节点
            java.nio.ByteBuffer buffer = java.nio.ByteBuffer.wrap(pageData);
            int magic = buffer.getInt();

            if (magic != BPlusTreeNode.MAGIC) {
                // 页未初始化或不是B+树节点,返回空节点
                return new BPlusTreeNode(true);
            }

            // 从字节数组反序列化节点
            BPlusTreeNode node = BPlusTreeNode.fromBytes(pageData);
            node.setPageId(pageId);

            return node;
        } finally {
            frame.unpin(false);
        }
    }

    /**
     * 保存节点到BufferPool
     *
     * @param node 节点对象
     */
    private void saveNode(BPlusTreeNode node) {
        int pageId = node.getPageId();

        // 获取页帧(如果不存在则创建)
        PageFrame frame;
        try {
            // 尝试获取已存在的页
            frame = bufferPool.getPage(indexId, pageId);
        } catch (IllegalArgumentException e) {
            // 页不存在,创建新页
            frame = bufferPool.newPage(indexId, pageId);
        }

        frame.pin();

        try {
            // 序列化节点到字节数组
            byte[] nodeData = node.toBytes();

            // 获取DataPage并直接写入数据
            DataPage page = (DataPage) frame.getPage();

            // ⚠️ 简化实现:直接替换页的底层字节数组
            // 生产环境:应该使用DataPage的insertRow方法存储节点
            byte[] pageData = page.getData();

            // 清零页数据(避免旧数据干扰)
            java.util.Arrays.fill(pageData, (byte) 0);

            // 复制节点数据到页
            System.arraycopy(nodeData, 0, pageData, 0, nodeData.length);

            // 标记为脏页
            frame.markDirty();
        } finally {
            frame.unpin(false);
        }
    }

    /**
     * 获取索引ID
     */
    public int getIndexId() {
        return indexId;
    }

    /**
     * 获取索引名称
     */
    public String getIndexName() {
        return indexName;
    }

    /**
     * 是否为聚簇索引
     */
    public boolean isClustered() {
        return isClustered;
    }

    /**
     * 获取索引列名
     */
    public String getColumnName() {
        return columnName;
    }

    /**
     * 获取树高度
     */
    public int getHeight() {
        return height;
    }

    @Override
    public String toString() {
        return "BPlusTree{" +
                "indexId=" + indexId +
                ", indexName='" + indexName + '\'' +
                ", isClustered=" + isClustered +
                ", columnName='" + columnName + '\'' +
                ", height=" + height +
                '}';
    }
}
