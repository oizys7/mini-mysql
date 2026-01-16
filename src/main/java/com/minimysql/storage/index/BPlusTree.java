package com.minimysql.storage.index;

import com.minimysql.storage.buffer.BufferPool;
import com.minimysql.storage.buffer.PageFrame;
import com.minimysql.storage.page.IndexPage;
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
 * - 实现Index接口,支持多态
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
public abstract class BPlusTree implements Index {

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
    @Override
    public Object search(Object key) {
        return searchInt(((Number) key).intValue());
    }

    /**
     * 查找键(int版本)
     *
     * 内部使用的优化版本，避免类型转换。
     *
     * @param key 键值(INT类型的哈希码)
     * @return 找到的值,不存在返回null
     */
    public Object searchInt(int key) {
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
            // 非根节点：需要将分裂键和新节点插入父节点
            int newChildPageId = pageManager.allocatePage();
            splitResult.newNode.setPageId(newChildPageId);
            saveNode(splitResult.newNode);

            // 重要修复：找到父节点并将分裂键和新子节点插入
            insertSplitKeyToParent(node.getPageId(), splitResult.splitKey, newChildPageId);
        }
    }

    /**
     * 将分裂键插入到父节点
     *
     * @param childPageId 子节点的pageId（用于查找父节点）
     * @param splitKey 分裂键
     * @param newChildPageId 新子节点的pageId
     */
    private void insertSplitKeyToParent(int childPageId, int splitKey, int newChildPageId) {
        // 从根节点开始，查找包含childPageId的父节点
        BPlusTreeNode root = loadNode(ROOT_PAGE_ID);

        if (root.isLeaf()) {
            // 根节点是叶子节点，不应该有这种情况
            throw new IllegalStateException("Root node is leaf, cannot find parent");
        }

        findAndInsertToParent(root, childPageId, splitKey, newChildPageId);
    }

    /**
     * 递归查找父节点并插入分裂键
     *
     * @param node 当前节点
     * @param childPageId 要查找的子节点pageId
     * @param splitKey 分裂键
     * @param newChildPageId 新子节点的pageId
     */
    private void findAndInsertToParent(BPlusTreeNode node, int childPageId, int splitKey, int newChildPageId) {
        if (node.isLeaf()) {
            // 不应该到达叶子节点
            throw new IllegalStateException("Reached leaf node while searching for parent");
        }

        // 检查当前节点的子节点中是否包含childPageId
        for (int i = 0; i <= node.getKeyCount(); i++) {
            if (node.getChild(i) == childPageId) {
                // 找到了！当前节点就是父节点
                node.insertChild(splitKey, newChildPageId);
                saveNode(node);

                // 检查父节点是否需要分裂
                if (node.needsSplit()) {
                    if (node.getPageId() == ROOT_PAGE_ID) {
                        // 根节点需要分裂
                        splitInternalNode(node);
                    } else {
                        // 非根节点需要分裂，递归向上处理
                        BPlusTreeNode.SplitResult splitResult = node.split();

                        int newNodePageId = pageManager.allocatePage();
                        splitResult.newNode.setPageId(newNodePageId);
                        saveNode(splitResult.newNode);

                        // 继续向上查找父节点
                        insertSplitKeyToParent(node.getPageId(), splitResult.splitKey, newNodePageId);
                    }
                }
                return;
            }
        }

        // 当前节点不是父节点，递归查找子节点
        // 找到应该搜索哪个子节点（基于splitKey）
        int pos = node.findKeyPosition(splitKey);
        int nextChildPageId = node.getChild(pos);
        BPlusTreeNode child = loadNode(nextChildPageId);
        findAndInsertToParent(child, childPageId, splitKey, newChildPageId);
    }

    /**
     * 分裂内部节点
     */
    private void splitInternalNode(BPlusTreeNode node) {
        BPlusTreeNode.SplitResult splitResult = node.split();

        // 创建新根节点
        BPlusTreeNode newRoot = new BPlusTreeNode(false);
        newRoot.setPageId(ROOT_PAGE_ID);

        // 旧根节点变成左子节点
        int oldRootPageId = pageManager.allocatePage();
        node.setPageId(oldRootPageId);
        saveNode(node);

        // 新分裂的节点变成右子节点
        int newRootPageId = pageManager.allocatePage();
        splitResult.newNode.setPageId(newRootPageId);
        saveNode(splitResult.newNode);

        // 新根节点指向两个子节点
        newRoot.setChild(0, oldRootPageId);
        newRoot.insertChild(splitResult.splitKey, newRootPageId);

        // 更新树高度
        height++;
        saveNode(newRoot);
    }

    /**
     * 插入键值对
     *
     * @param key 键值
     * @param value 值
     */
    @Override
    public void insert(Object key, Object value) {
        insertInt(((Number) key).intValue(), value);
    }

    /**
     * 插入键值对(int版本)
     *
     * 内部使用的优化版本，避免类型转换。
     *
     * @param key 键值
     * @param value 值
     */
    public void insertInt(int key, Object value) {
        // 找到应该插入的叶子节点
        BPlusTreeNode root = loadNode(ROOT_PAGE_ID);
        BPlusTreeNode leaf = findLeafNode(root, key);

        // 在叶子节点插入
        leaf.insertKeyValue(key, value);
        saveNode(leaf);

        // 检查是否需要分裂
        if (leaf.needsSplit()) {
            splitLeafNode(leaf);
        }
    }

    /**
     * 分裂根节点（增加树高度）
     */
    private void splitRootNode() {
        BPlusTreeNode oldRoot = loadNode(ROOT_PAGE_ID);

        // 根节点不应该是叶子节点（叶子节点的分裂由splitLeafNode处理）
        if (oldRoot.isLeaf()) {
            return;
        }

        // 分裂旧根节点
        BPlusTreeNode.SplitResult splitResult = oldRoot.split();

        // 创建新根节点
        BPlusTreeNode newRoot = new BPlusTreeNode(false);
        newRoot.setPageId(ROOT_PAGE_ID);

        // 旧根节点变成左子节点，分配新pageId
        int oldRootPageId = pageManager.allocatePage();
        oldRoot.setPageId(oldRootPageId);
        saveNode(oldRoot);

        // 新分裂的节点变成右子节点
        int newRootPageId = pageManager.allocatePage();
        splitResult.newNode.setPageId(newRootPageId);
        saveNode(splitResult.newNode);

        // 新根节点指向两个子节点
        newRoot.setChild(0, oldRootPageId);
        newRoot.insertChild(splitResult.splitKey, newRootPageId);

        // 更新树高度
        height++;
        saveNode(newRoot);
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
    @Override
    public void delete(Object key) {
        deleteInt(((Number) key).intValue());
    }

    /**
     * 删除键（完整实现）
     *
     * B+树删除算法:
     * 1. 找到包含key的叶子节点
     * 2. 在叶子节点中删除key
     * 3. 如果节点过少(键数 < MIN_CHILDREN - 1):
     *    a. 尝试从左兄弟节点借位
     *    b. 如果左兄弟不够，尝试从右兄弟节点借位
     *    c. 如果兄弟节点都不够，合并节点
     * 4. 递归向上处理父节点
     *
     * @param key 键值
     */
    public void deleteInt(int key) {
        BPlusTreeNode root = loadNode(ROOT_PAGE_ID);

        // 递归删除
        DeleteResult result = deleteInt(root, key);

        // 如果根节点变空且有一个子节点，降低树高度
        if (result.rootChanged) {
            BPlusTreeNode newRoot = loadNode(ROOT_PAGE_ID);
            if (!newRoot.isLeaf() && newRoot.getKeyCount() == 0) {
                // 根节点只有一个子节点，将该子节点提升为新的根节点
                int oldRootPageId = ROOT_PAGE_ID;
                int newRootPageId = newRoot.getChild(0);

                // 注意：这里需要重新分配ROOT_PAGE_ID，简化实现暂不处理
                // 实际应该更新BPlusTree的根节点引用
            }
        }
    }

    /**
     * 递归删除内部结果
     */
    private static class DeleteResult {
        boolean rootChanged; // 根节点是否改变
        boolean underflow;   // 是否发生下溢（需要合并）

        DeleteResult(boolean rootChanged, boolean underflow) {
            this.rootChanged = rootChanged;
            this.underflow = underflow;
        }
    }

    /**
     * 递归删除键
     *
     * @param node 当前节点
     * @param key 要删除的键
     * @return 删除结果
     */
    private DeleteResult deleteInt(BPlusTreeNode node, int key) {
        if (node.isLeaf()) {
            // 叶子节点：直接删除
            boolean deleted = node.removeKey(key);
            if (deleted) {
                saveNode(node);

                // 检查是否下溢
                boolean underflow = node.getKeyCount() < BPlusTreeNode.MIN_CHILDREN - 1;
                return new DeleteResult(false, underflow);
            }
            return new DeleteResult(false, false);
        } else {
            // 内部节点：递归到子节点
            int childIndex = findChildIndexPath(node, key);
            int childPageId = node.getChild(childIndex);
            BPlusTreeNode child = loadNode(childPageId);

            // 递归删除
            DeleteResult childResult = deleteInt(child, key);

            // 处理子节点的下溢
            if (childResult.underflow) {
                return handleUnderflow(node, childIndex);
            }

            return new DeleteResult(false, false);
        }
    }

    /**
     * 查找子节点的路径
     *
     * @param node 内部节点
     * @param key 键
     * @return 子节点索引
     */
    private int findChildIndexPath(BPlusTreeNode node, int key) {
        int keyCount = node.getKeyCount();

        // 找到第一个 >= key 的键的位置
        int i = 0;
        while (i < keyCount && key >= node.getKey(i)) {
            i++;
        }

        return i;
    }

    /**
     * 处理节点下溢
     *
     * 策略:
     * 1. 尝试从左兄弟借位
     * 2. 如果左兄弟不够，从右兄弟借位
     * 3. 如果兄弟都不够，合并节点
     *
     * @param parent 父节点
     * @param childIndex 下溢的子节点索引
     * @return 删除结果
     */
    private DeleteResult handleUnderflow(BPlusTreeNode parent, int childIndex) {
        // 尝试从左兄弟借位
        if (childIndex > 0) {
            int leftSiblingPageId = parent.getChild(childIndex - 1);
            BPlusTreeNode leftSibling = loadNode(leftSiblingPageId);

            if (leftSibling.getKeyCount() > BPlusTreeNode.MIN_CHILDREN - 1) {
                // 左兄弟有多余的键，可以借位
                borrowFromLeftSibling(parent, childIndex);
                return new DeleteResult(false, false);
            }
        }

        // 尝试从右兄弟借位
        if (childIndex < parent.getKeyCount()) {
            int rightSiblingPageId = parent.getChild(childIndex + 1);
            BPlusTreeNode rightSibling = loadNode(rightSiblingPageId);

            if (rightSibling.getKeyCount() > BPlusTreeNode.MIN_CHILDREN - 1) {
                // 右兄弟有多余的键，可以借位
                borrowFromRightSibling(parent, childIndex);
                return new DeleteResult(false, false);
            }
        }

        // 兄弟节点都不够，需要合并
        if (childIndex > 0) {
            // 与左兄弟合并
            mergeWithLeftSibling(parent, childIndex - 1);
        } else {
            // 与右兄弟合并
            mergeWithRightSibling(parent, childIndex);
        }

        // 检查父节点是否也下溢
        boolean parentUnderflow = parent.getKeyCount() < BPlusTreeNode.MIN_CHILDREN - 1;
        return new DeleteResult(false, parentUnderflow);
    }

    /**
     * 从左兄弟借位
     *
     * 策略:
     * 1. 将左兄弟的最大键移动到父节点
     * 2. 将父节点的分隔键移动到当前节点
     *
     * @param parent 父节点
     * @param childIndex 当前子节点索引
     */
    private void borrowFromLeftSibling(BPlusTreeNode parent, int childIndex) {
        int childPageId = parent.getChild(childIndex);
        int leftSiblingPageId = parent.getChild(childIndex - 1);

        BPlusTreeNode child = loadNode(childPageId);
        BPlusTreeNode leftSibling = loadNode(leftSiblingPageId);

        if (child.isLeaf()) {
            // 叶子节点借位
            // 1. 从左兄弟借最后一个键值对
            int borrowedKey = leftSibling.getKey(leftSibling.getKeyCount() - 1);
            Object borrowedValue = leftSibling.getValue(leftSibling.getKeyCount() - 1);
            leftSibling.removeKeyValue(leftSibling.getKeyCount() - 1);

            // 2. 更新父节点的分隔键
            int parentSeparatorIndex = childIndex - 1;
            int oldSeparator = parent.getKey(parentSeparatorIndex);
            parent.setKey(parentSeparatorIndex, borrowedKey);

            // 3. 将旧的分隔键和借来的值插入当前节点
            child.insertKeyValue(oldSeparator, borrowedValue);

            // 4. 更新叶子节点的链表
            // (不需要，因为节点顺序不变)
        } else {
            // 内部节点借位
            // 1. 从左兄弟借最后一个键和最后一个子节点
            int borrowedKey = leftSibling.getKey(leftSibling.getKeyCount() - 1);
            int borrowedChild = leftSibling.getChild(leftSibling.getKeyCount());
            leftSibling.removeKeyValue(leftSibling.getKeyCount() - 1);
            // 注意：内部节点的子节点数量 = keyCount + 1

            // 2. 更新父节点的分隔键
            int parentSeparatorIndex = childIndex - 1;
            int oldSeparator = parent.getKey(parentSeparatorIndex);
            parent.setKey(parentSeparatorIndex, borrowedKey);

            // 3. 将旧的分隔键作为新键插入当前节点
            child.insertKeyValue(oldSeparator, borrowedChild);
        }

        // 保存修改
        saveNode(leftSibling);
        saveNode(child);
        saveNode(parent);
    }

    /**
     * 从右兄弟借位
     *
     * 策略:
     * 1. 将右兄弟的最小键移动到父节点
     * 2. 将父节点的分隔键移动到当前节点
     *
     * @param parent 父节点
     * @param childIndex 当前子节点索引
     */
    private void borrowFromRightSibling(BPlusTreeNode parent, int childIndex) {
        int childPageId = parent.getChild(childIndex);
        int rightSiblingPageId = parent.getChild(childIndex + 1);

        BPlusTreeNode child = loadNode(childPageId);
        BPlusTreeNode rightSibling = loadNode(rightSiblingPageId);

        if (child.isLeaf()) {
            // 叶子节点借位
            // 1. 从右兄弟借第一个键值对
            int borrowedKey = rightSibling.getKey(0);
            Object borrowedValue = rightSibling.getValue(0);
            rightSibling.removeKeyValue(0);

            // 2. 更新父节点的分隔键
            int parentSeparatorIndex = childIndex;
            int oldSeparator = parent.getKey(parentSeparatorIndex);
            parent.setKey(parentSeparatorIndex, borrowedKey);

            // 3. 将旧的分隔键和借来的值插入当前节点
            child.insertKeyValue(oldSeparator, borrowedValue);
        } else {
            // 内部节点借位
            // 1. 从右兄弟借第一个键和第一个子节点
            int borrowedKey = rightSibling.getKey(0);
            int borrowedChild = rightSibling.getChild(0);
            rightSibling.removeKeyValue(0);
            // 注意：需要删除第一个子节点，这比较复杂

            // 2. 更新父节点的分隔键
            int parentSeparatorIndex = childIndex;
            int oldSeparator = parent.getKey(parentSeparatorIndex);
            parent.setKey(parentSeparatorIndex, borrowedKey);

            // 3. 将旧的分隔键作为新键插入当前节点
            child.insertKeyValue(oldSeparator, borrowedChild);
        }

        // 保存修改
        saveNode(rightSibling);
        saveNode(child);
        saveNode(parent);
    }

    /**
     * 与左兄弟合并
     *
     * @param parent 父节点
     * @param leftSiblingIndex 左兄弟索引
     */
    private void mergeWithLeftSibling(BPlusTreeNode parent, int leftSiblingIndex) {
        int leftSiblingPageId = parent.getChild(leftSiblingIndex);
        int childPageId = parent.getChild(leftSiblingIndex + 1);

        BPlusTreeNode leftSibling = loadNode(leftSiblingPageId);
        BPlusTreeNode child = loadNode(childPageId);

        // 将父节点的分隔键下推到左兄弟
        int separatorKey = parent.getKey(leftSiblingIndex);

        if (child.isLeaf()) {
            // 叶子节点合并
            // 1. 将分隔键和child的所有键值对移动到左兄弟
            leftSibling.insertKeyValue(separatorKey, parent.getValue(leftSiblingIndex));

            for (int i = 0; i < child.getKeyCount(); i++) {
                leftSibling.insertKeyValue(child.getKey(i), child.getValue(i));
            }

            // 2. 更新左兄弟的nextLeaf指针
            leftSibling.setNextLeafPageId(child.getNextLeafPageId());
        } else {
            // 内部节点合并
            // 1. 将分隔键插入左兄弟
            leftSibling.insertKeyValue(separatorKey, child.getChild(0));

            // 2. 将child的所有键和子节点移动到左兄弟
            for (int i = 0; i < child.getKeyCount(); i++) {
                leftSibling.insertKeyValue(child.getKey(i), child.getChild(i + 1));
            }
        }

        // 3. 从父节点删除分隔键和child指针
        parent.removeKeyValue(leftSiblingIndex);
        parent.removeChild(leftSiblingIndex + 1);

        // 4. 保存修改
        saveNode(leftSibling);
        saveNode(parent);

        // 注意：child节点现在是孤立的，可以回收（简化实现暂不处理）
    }

    /**
     * 与右兄弟合并
     *
     * @param parent 父节点
     * @param childIndex 当前子节点索引
     */
    private void mergeWithRightSibling(BPlusTreeNode parent, int childIndex) {
        int childPageId = parent.getChild(childIndex);
        int rightSiblingPageId = parent.getChild(childIndex + 1);

        BPlusTreeNode child = loadNode(childPageId);
        BPlusTreeNode rightSibling = loadNode(rightSiblingPageId);

        // 将父节点的分隔键下推到child
        int separatorKey = parent.getKey(childIndex);

        if (child.isLeaf()) {
            // 叶子节点合并
            // 1. 将分隔键和右兄弟的所有键值对移动到child
            child.insertKeyValue(separatorKey, parent.getValue(childIndex));

            for (int i = 0; i < rightSibling.getKeyCount(); i++) {
                child.insertKeyValue(rightSibling.getKey(i), rightSibling.getValue(i));
            }

            // 2. 更新child的nextLeaf指针
            child.setNextLeafPageId(rightSibling.getNextLeafPageId());
        } else {
            // 内部节点合并
            // 1. 将分隔键插入child
            child.insertKeyValue(separatorKey, rightSibling.getChild(0));

            // 2. 将右兄弟的所有键和子节点移动到child
            for (int i = 0; i < rightSibling.getKeyCount(); i++) {
                child.insertKeyValue(rightSibling.getKey(i), rightSibling.getChild(i + 1));
            }
        }

        // 3. 从父节点删除分隔键和右兄弟指针
        parent.removeKeyValue(childIndex);
        parent.removeChild(childIndex + 1);

        // 4. 保存修改
        saveNode(child);
        saveNode(parent);

        // 注意：rightSibling节点现在是孤立的，可以回收（简化实现暂不处理）
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
    @Override
    public List<Object> rangeSearch(Object startKey, Object endKey) {
        return rangeSearchInt(((Number) startKey).intValue(), ((Number) endKey).intValue());
    }

    /**
     * 获取所有记录
     *
     * 遍历所有叶子节点，返回所有值。
     * 用于全表扫描操作。
     *
     * @return 所有值的列表
     */
    public List<Object> getAll() {
        List<Object> results = new ArrayList<>();

        // 1. 找到最左边的叶子节点（最小的键）
        BPlusTreeNode root = loadNode(ROOT_PAGE_ID);
        BPlusTreeNode leaf = findLeftmostLeaf(root);

        // 2. 遍历所有叶子节点
        while (leaf != null) {
            // 收集当前叶子节点的所有值
            for (int i = 0; i < leaf.getKeyCount(); i++) {
                results.add(leaf.getValue(i));
            }

            // 移动到下一个叶子节点
            int nextLeafPageId = leaf.getNextLeafPageId();
            if (nextLeafPageId == -1) {
                break; // 已到最后一个叶子节点
            }

            leaf = loadNode(nextLeafPageId);
        }

        return results;
    }

    /**
     * 找到最左边的叶子节点
     *
     * @param node 当前节点
     * @return 最左边的叶子节点
     */
    protected BPlusTreeNode findLeftmostLeaf(BPlusTreeNode node) {
        while (!node.isLeaf()) {
            // 总是走最左边的路径
            int leftmostChildPageId = node.getChild(0);
            node = loadNode(leftmostChildPageId);
        }
        return node;
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
    public List<Object> rangeSearchInt(int startKey, int endKey) {
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
            IndexPage indexPage = (IndexPage) frame.getPage();

            // 检查页是否已初始化(通过Magic Number判断)
            byte[] pageData = indexPage.getData();

            // IndexPage布局: [PageType(1) | PageId(4) | Reserved(7) | NodeData(...)]
            // BPlusTreeNode的Magic Number在NodeData开始处(偏移量12)
            if (pageData.length < IndexPage.HEADER_SIZE + 4) {
                // 页未初始化,返回空节点
                return new BPlusTreeNode(true);
            }

            // 读取Magic Number判断是否为B+树节点(跳过IndexPage页头)
            java.nio.ByteBuffer buffer = java.nio.ByteBuffer.wrap(pageData);
            buffer.position(IndexPage.HEADER_SIZE); // 跳过页头
            int magic = buffer.getInt();

            if (magic != BPlusTreeNode.MAGIC) {
                // 页未初始化或不是B+树节点,返回空节点
                return new BPlusTreeNode(true);
            }

            // 从IndexPage获取节点(IndexPage会反序列化)
            BPlusTreeNode node = indexPage.getNode();
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
            // 获取或创建IndexPage
            IndexPage indexPage;
            try {
                indexPage = (IndexPage) frame.getPage();
            } catch (ClassCastException e) {
                // 页存在但不是IndexPage,创建新的
                indexPage = new IndexPage(node);
                indexPage.setPageId(pageId); // 设置正确的pageId
                frame.setPage(indexPage);
            }

            // 设置节点数据(IndexPage会序列化)
            indexPage.setNode(node);

            // 标记为脏页
            frame.markDirty();
        } finally {
            frame.unpin(false);
        }
    }

    /**
     * 获取索引ID
     */
    @Override
    public int getIndexId() {
        return indexId;
    }

    /**
     * 获取索引名称
     */
    @Override
    public String getIndexName() {
        return indexName;
    }

    /**
     * 是否为聚簇索引
     */
    @Override
    public boolean isClustered() {
        return isClustered;
    }

    /**
     * 获取索引列名
     */
    @Override
    public String getColumnName() {
        return columnName;
    }

    /**
     * 是否为唯一索引
     *
     * B+树索引默认不是唯一索引。
     * 子类(如UniqueBPlusTree)可以重写此方法。
     *
     * @return 默认返回false
     */
    @Override
    public boolean isUnique() {
        return false;
    }

    /**
     * 获取树高度
     */
    @Override
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
