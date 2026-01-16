package com.minimysql.storage.index;

import com.minimysql.storage.table.Column;
import com.minimysql.storage.table.Row;


/**
 * BPlusTreeNode - B+树节点
 *
 * B+树节点可以是内部节点或叶子节点,统一用isLeaf区分。
 * 节点存储在DataPage中,大小固定为16KB。
 *
 * 节点布局:
 * - 内部节点: [key0, key1, ..., keyN] + [child0, child1, ..., childN+1]
 * - 叶子节点: [key0, key1, ..., keyN] + [value0, value1, ..., valueN] + nextLeaf
 *
 * 设计原则(MySQL兼容):
 * - 节点大小=页大小(16KB),对应InnoDB的索引页
 * - 内部节点的value指向子节点pageId
 * - 叶子节点的value指向实际数据(Row或主键)
 * - 叶子节点通过nextLeaf形成有序链表(范围查询)
 * - 支持INT和VARCHAR两种键类型
 *
 * "Good taste": 内部节点和叶子节点统一结构,消除特殊情况
 *
 * B+树 vs B树:
 * - B+树所有数据在叶子节点,内部节点只存索引
 * - B+树叶子节点形成链表,支持范围查询
 * - B+树查询效率稳定(总是从根到叶)
 */
public class BPlusTreeNode {

    /** 最大子节点数(阶数) */
    public static final int MAX_CHILDREN = 100; // 可根据页大小调整

    /** 最小子节点数(阶数/2,保证平衡) */
    public static final int MIN_CHILDREN = MAX_CHILDREN / 2;

    /** 是否为叶子节点 */
    private boolean isLeaf;

    /** 键的数量 */
    private int keyCount;

    /** 键数组(INT类型或VARCHAR的哈希码) */
    private final int[] keys;

    /** 值数组
     *  - 内部节点: 子节点pageId (Integer)
     *  - 叶子节点(聚簇索引): Row数据
     *  - 叶子节点(二级索引): 主键值 (Integer)
     */
    private final Object[] values;

    /** 叶子节点链表:指向下一个叶子节点的pageId */
    private int nextLeafPageId;

    /** 当前节点所在的pageId */
    private int pageId;

    /** 值类型(仅叶子节点): 0=INT(pageId), 1=BYTES(Row数据) */
    private byte valueType;

    /** 值类型常量 */
    public static final byte VALUE_TYPE_INT = 0;
    public static final byte VALUE_TYPE_BYTES = 1;

    /**
     * 创建新节点
     *
     * @param isLeaf 是否为叶子节点
     */
    public BPlusTreeNode(boolean isLeaf) {
        this.isLeaf = isLeaf;
        this.keyCount = 0;
        this.keys = new int[MAX_CHILDREN - 1]; // m个key
        this.values = new Object[MAX_CHILDREN]; // m+1个child或m个value
        this.nextLeafPageId = -1;
        this.pageId = -1;
        this.valueType = VALUE_TYPE_INT; // 默认INT类型
    }

    /**
     * 查找键的位置
     *
     * 使用二分查找,返回键应该插入的位置。
     *
     * @param key 要查找的键
     * @return 键的位置(0~keyCount),如果存在返回对应位置
     */
    public int findKeyPosition(int key) {
        int left = 0;
        int right = keyCount - 1;

        while (left <= right) {
            int mid = (left + right) / 2;

            if (keys[mid] == key) {
                return mid;
            } else if (keys[mid] < key) {
                left = mid + 1;
            } else {
                right = mid - 1;
            }
        }

        return left;
    }

    /**
     * 获取键
     */
    public int getKey(int index) {
        if (index < 0 || index >= keyCount) {
            throw new IndexOutOfBoundsException("Key index out of bounds: " + index);
        }
        return keys[index];
    }

    /**
     * 设置键
     */
    public void setKey(int index, int key) {
        if (index < 0 || index >= keys.length) {
            throw new IndexOutOfBoundsException("Key index out of bounds: " + index);
        }
        keys[index] = key;
    }

    /**
     * 获取值
     */
    public Object getValue(int index) {
        if (index < 0 || index >= keyCount + 1) {
            throw new IndexOutOfBoundsException("Value index out of bounds: " + index);
        }
        return values[index];
    }

    /**
     * 设置值
     */
    public void setValue(int index, Object value) {
        if (index < 0 || index >= values.length) {
            throw new IndexOutOfBoundsException("Value index out of bounds: " + index);
        }
        values[index] = value;
    }

    /**
     * 插入键值对(叶子节点)
     *
     * @param key 键
     * @param value 值
     * @return 插入位置
     */
    public int insertKeyValue(int key, Object value) {
        if (!isLeaf) {
            throw new IllegalStateException("Only leaf nodes can insert key-value pairs");
        }

        int pos = findKeyPosition(key);

        // 移动现有键和值
        for (int i = keyCount; i > pos; i--) {
            keys[i] = keys[i - 1];
            values[i] = values[i - 1];
        }

        // 插入新键值
        keys[pos] = key;
        values[pos] = value;
        keyCount++;

        return pos;
    }

    /**
     * 插入子节点(内部节点)
     *
     * @param key 分隔键
     * @param child 子节点pageId
     * @return 插入位置
     */
    public int insertChild(int key, int child) {
        if (isLeaf) {
            throw new IllegalStateException("Only internal nodes can insert children");
        }

        int pos = findKeyPosition(key);

        // 移动现有键和子节点
        for (int i = keyCount; i > pos; i--) {
            keys[i] = keys[i - 1];
            values[i + 1] = values[i];
        }

        // 插入新键和子节点
        keys[pos] = key;
        values[pos + 1] = child;
        keyCount++;

        return pos;
    }

    /**
     * 删除键值对
     *
     * @param index 要删除的键的位置
     */
    public void removeKeyValue(int index) {
        if (!isLeaf) {
            throw new IllegalStateException("Only leaf nodes can remove key-value pairs");
        }

        if (index < 0 || index >= keyCount) {
            throw new IndexOutOfBoundsException("Index out of bounds: " + index);
        }

        // 移动现有键和值
        for (int i = index; i < keyCount - 1; i++) {
            keys[i] = keys[i + 1];
            values[i] = values[i + 1];
        }

        keyCount--;
    }

    /**
     * 删除子节点
     *
     * @param index 要删除的键的位置
     */
    public void removeChild(int index) {
        if (isLeaf) {
            throw new IllegalStateException("Only internal nodes can remove children");
        }

        if (index < 0 || index >= keyCount) {
            throw new IndexOutOfBoundsException("Index out of bounds: " + index);
        }

        // 移动现有键和子节点
        for (int i = index; i < keyCount - 1; i++) {
            keys[i] = keys[i + 1];
            values[i + 1] = values[i + 2];
        }

        keyCount--;
    }

    /**
     * 分裂节点
     *
     * 当节点满时,分裂成两个节点,返回中间键和新节点。
     *
     * B+树分裂逻辑(MySQL InnoDB):
     * - 叶子节点: 平分键值对,中间键提升到父节点,新节点插入链表
     * - 内部节点: 中间键提升到父节点,右边键+右边所有子节点移到新节点
     *
     * 示例(叶子节点, MAX_CHILDREN=4, keyCount=3):
     *   原节点: [k0,v0] [k1,v1] [k2,v2]
     *   mid=1, splitKey=k1
     *   原节点变成: [k0,v0]
     *   新节点: [k2,v2], nextLeaf→原节点的nextLeaf
     *   返回: (k1, 新节点)
     *
     * @return [分裂出的键, 新节点]
     */
    public SplitResult split() {
        int mid = keyCount / 2;
        int splitKey = keys[mid];

        BPlusTreeNode newNode = new BPlusTreeNode(isLeaf);
        newNode.pageId = -1; // 新节点尚未分配pageId

        if (isLeaf) {
            // 叶子节点分裂: 平分键值对
            // 原节点: [k0,v0] [k1,v1] [k2,v2] [k3,v3]
            // mid=2, splitKey=k2
            // 原节点保留: [k0,v0] [k1,v1]
            // 新节点获得: [k3,v3]
            int newKeyCount = keyCount - mid - 1;
            newNode.keyCount = newKeyCount;

            // 复制键: keys[mid+1 ... keyCount-1] → newNode.keys[0 ... newKeyCount-1]
            System.arraycopy(keys, mid + 1, newNode.keys, 0, newKeyCount);
            // 复制值: values[mid+1 ... keyCount-1] → newNode.values[0 ... newKeyCount-1]
            System.arraycopy(values, mid + 1, newNode.values, 0, newKeyCount);

            // 更新链表指针: 原节点 → 新节点 → 原nextLeaf
            newNode.nextLeafPageId = this.nextLeafPageId;
            this.nextLeafPageId = newNode.pageId; // 稍后在BPlusTree中分配pageId后更新

        } else {
            // 内部节点分裂: 中间键提升,右边键+子节点移到新节点
            // 原节点: [k0] [k1] [k2] [k3] + [c0] [c1] [c2] [c3] [c4]
            // mid=2, splitKey=k2 (提升到父节点)
            // 原节点保留: [k0] [k1] + [c0] [c1] [c2]
            // 新节点获得: [k3] + [c3] [c4]
            int newKeyCount = keyCount - mid - 1;
            newNode.keyCount = newKeyCount;

            // 复制键: keys[mid+1 ... keyCount-1] → newNode.keys[0 ... newKeyCount-1]
            System.arraycopy(keys, mid + 1, newNode.keys, 0, newKeyCount);
            // 复制子节点: values[mid+1 ... keyCount] → newNode.values[0 ... newKeyCount]
            // 注意:内部节点values是子节点指针,数量=keyCount+1
            System.arraycopy(values, mid + 1, newNode.values, 0, newKeyCount + 1);
        }

        // 更新当前节点的键数量(去掉分裂出去的部分)
        keyCount = mid;

        return new SplitResult(splitKey, newNode);
    }

    /**
     * 分裂结果
     */
    public static class SplitResult {
        public final int splitKey;
        public final BPlusTreeNode newNode;

        public SplitResult(int splitKey, BPlusTreeNode newNode) {
            this.splitKey = splitKey;
            this.newNode = newNode;
        }
    }

    /**
     * 检查节点是否需要分裂
     */
    public boolean needsSplit() {
        return keyCount >= MAX_CHILDREN - 1;
    }

    /**
     * 检查节点是否需要合并
     */
    public boolean needsMerge() {
        return keyCount < MIN_CHILDREN - 1;
    }

    /**
     * 是否为叶子节点
     */
    public boolean isLeaf() {
        return isLeaf;
    }

    /**
     * 设置叶子节点标志
     */
    public void setLeaf(boolean leaf) {
        this.isLeaf = leaf;
    }

    /**
     * 获取键数量
     */
    public int getKeyCount() {
        return keyCount;
    }

    /**
     * 设置键数量
     */
    public void setKeyCount(int keyCount) {
        this.keyCount = keyCount;
    }

    /**
     * 获取下一个叶子节点pageId
     */
    public int getNextLeafPageId() {
        return nextLeafPageId;
    }

    /**
     * 设置下一个叶子节点pageId
     */
    public void setNextLeafPageId(int nextLeafPageId) {
        this.nextLeafPageId = nextLeafPageId;
    }

    /**
     * 获取当前节点的pageId
     */
    public int getPageId() {
        return pageId;
    }

    /**
     * 设置当前节点的pageId
     */
    public void setPageId(int pageId) {
        this.pageId = pageId;
    }

    /**
     * 获取子节点pageId(内部节点)
     */
    @SuppressWarnings("unchecked")
    public int getChild(int index) {
        if (isLeaf) {
            throw new IllegalStateException("Leaf nodes have no children");
        }
        return (Integer) values[index];
    }

    /**
     * 设置子节点pageId(内部节点)
     *
     * @param index 子节点索引
     * @param childPageId 子节点pageId
     */
    public void setChild(int index, int childPageId) {
        if (isLeaf) {
            throw new IllegalStateException("Leaf nodes have no children");
        }
        values[index] = childPageId;
    }

    /**
     * 获取值类型
     */
    public byte getValueType() {
        return valueType;
    }

    /**
     * 设置值类型
     */
    public void setValueType(byte valueType) {
        this.valueType = valueType;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Node{")
                .append("pageId=").append(pageId)
                .append(", isLeaf=").append(isLeaf)
                .append(", keys=[");
        for (int i = 0; i < keyCount; i++) {
            if (i > 0) sb.append(", ");
            sb.append(keys[i]);
        }
        sb.append("], keyCount=").append(keyCount);

        if (isLeaf) {
            sb.append(", nextLeaf=").append(nextLeafPageId);
        }

        sb.append("}");
        return sb.toString();
    }

    // ==================== 序列化和反序列化 ====================

    /**
     * 节点序列化格式(MySQL兼容):
     *
     * +------------------+ <- 0
     * | Magic (4 bytes)  |  0x4254504E ("BPTN" = BPlusTreeNode)
     * +------------------+ <- 4
     * | Version (1 byte) |  当前版本=1
     * +------------------+ <- 5
     * | Flags (1 byte)   |  bit0: isLeaf, bit1: valueType(0=INT, 1=BYTES)
     * +------------------+ <- 6
     * | Reserved (2)     |
     * +------------------+ <- 8
     * | keyCount (4)     |
     * +------------------+ <- 12
     * | nextLeafPageId (4)| 仅叶子节点
     * +------------------+ <- 16
     * | keys[] (4N)      |  每个key 4字节
     * +------------------+
     * | values[] (4N/VAR)|  内部节点:子节点pageId(4字节)
     *                   |  叶子节点(valueType=0):主键值(4字节)
     *                   |  叶子节点(valueType=1):Row序列化数据(变长)
     * +------------------+
     *
     * 设计原则:
     * - 固定头部便于快速读取节点元信息
     * - keys连续存储,利用CPU缓存
     * - values根据类型灵活存储
     * - 支持快速二分查找
     */

    /** Magic Number: "BPTN" (BPlusTreeNode) */
    public static final int MAGIC = 0x4254504E;

    /** 当前版本 */
    private static final int VERSION = 1;

    /**
     * 序列化节点到字节数组
     *
     * ⚠️ 简化实现:只支持INT类型的键和值
     * 生产环境:需要支持VARCHAR、Row序列化等
     *
     * @return 字节数组
     */
    public byte[] toBytes() {
        try {
            java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
            java.nio.ByteBuffer buffer;

            // 1. 写入Magic Number (4 bytes)
            buffer = java.nio.ByteBuffer.allocate(4);
            buffer.putInt(MAGIC);
            baos.write(buffer.array());

            // 2. 写入版本 (1 byte)
            baos.write(VERSION);

            // 3. 写入标志位 (1 byte)
            byte flags = 0;
            if (isLeaf) {
                flags |= 0x01;
            }
            if (valueType == VALUE_TYPE_BYTES) {
                flags |= 0x02; // bit1: 值类型为BYTES
            }
            baos.write(flags);

            // 4. 写入保留字段 (2 bytes)
            baos.write(0);
            baos.write(0);

            // 5. 写入keyCount (4 bytes)
            buffer = java.nio.ByteBuffer.allocate(4);
            buffer.putInt(keyCount);
            baos.write(buffer.array());

            // 6. 写入nextLeafPageId (4 bytes, 仅叶子节点)
            buffer = java.nio.ByteBuffer.allocate(4);
            buffer.putInt(nextLeafPageId);
            baos.write(buffer.array());

            // 7. 写入keys数组 (4 bytes * keyCount)
            for (int i = 0; i < keyCount; i++) {
                buffer = java.nio.ByteBuffer.allocate(4);
                buffer.putInt(keys[i]);
                baos.write(buffer.array());
            }

            // 8. 写入values数组
            if (isLeaf) {
                // 叶子节点:values存储数据(简化:只存储Integer)
                // 生产环境:需要存储Row序列化数据
                for (int i = 0; i < keyCount; i++) {
                    Object value = values[i];
                    if (value instanceof Integer) {
                        buffer = java.nio.ByteBuffer.allocate(4);
                        buffer.putInt((Integer) value);
                        baos.write(buffer.array());
                    } else if (value instanceof byte[]) {
                        // Row数据(字节数组)
                        byte[] rowBytes = (byte[]) value;
                        // 先写入长度(2 bytes)
                        buffer = java.nio.ByteBuffer.allocate(2);
                        buffer.putShort((short) rowBytes.length);
                        baos.write(buffer.array());
                        // 再写入数据
                        baos.write(rowBytes);
                    } else {
                        throw new IllegalArgumentException(
                                "Unsupported value type: " + value.getClass());
                    }
                }
            } else {
                // 内部节点:values存储子节点pageId
                for (int i = 0; i < keyCount + 1; i++) {
                    buffer = java.nio.ByteBuffer.allocate(4);
                    buffer.putInt((Integer) values[i]);
                    baos.write(buffer.array());
                }
            }

            return baos.toByteArray();
        } catch (java.io.IOException e) {
            throw new RuntimeException("Failed to serialize node", e);
        }
    }

    /**
     * 从字节数组反序列化节点
     *
     * @param data 字节数组
     * @return BPlusTreeNode对象
     */
    public static BPlusTreeNode fromBytes(byte[] data) {
        java.nio.ByteBuffer buffer = java.nio.ByteBuffer.wrap(data);

        // 1. 读取并验证Magic Number
        int magic = buffer.getInt();
        if (magic != MAGIC) {
            throw new IllegalArgumentException(
                    "Invalid magic number: 0x" + Integer.toHexString(magic));
        }

        // 2. 读取版本
        int version = buffer.get();
        if (version != VERSION) {
            throw new IllegalArgumentException(
                    "Unsupported version: " + version);
        }

        // 3. 读取标志位
        byte flags = buffer.get();
        boolean isLeaf = (flags & 0x01) != 0;
        boolean isBytesValue = (flags & 0x02) != 0;

        // 4. 跳过保留字段 (2 bytes)
        buffer.getShort();

        // 5. 读取keyCount
        int keyCount = buffer.getInt();

        // 6. 读取nextLeafPageId
        int nextLeafPageId = buffer.getInt();

        // 创建节点
        BPlusTreeNode node = new BPlusTreeNode(isLeaf);
        node.keyCount = keyCount;
        node.nextLeafPageId = nextLeafPageId;

        // 设置值类型
        node.valueType = isBytesValue ? VALUE_TYPE_BYTES : VALUE_TYPE_INT;

        // 7. 读取keys数组
        for (int i = 0; i < keyCount; i++) {
            node.keys[i] = buffer.getInt();
        }

        // 8. 读取values数组
        if (isLeaf) {
            // 叶子节点
            if (node.valueType == VALUE_TYPE_BYTES) {
                // BYTES类型:Row数据
                for (int i = 0; i < keyCount; i++) {
                    // 读取长度(2 bytes)
                    short len = buffer.getShort();
                    // 读取数据
                    byte[] rowBytes = new byte[len];
                    buffer.get(rowBytes);
                    node.values[i] = rowBytes;
                }
            } else {
                // INT类型:主键值
                for (int i = 0; i < keyCount; i++) {
                    node.values[i] = buffer.getInt();
                }
            }
        } else {
            // 内部节点:子节点pageId
            for (int i = 0; i < keyCount + 1; i++) {
                node.values[i] = buffer.getInt();
            }
        }

        return node;
    }

    /**
     * 序列化叶子节点的Row数据(辅助方法)
     *
     * ⚠️ 简化实现:假设Row已经序列化为byte[]
     * 生产环境:应该直接存储Row的字节数组
     *
     * @param row Row对象
     * @return 字节数组
     */
    public static byte[] serializeRow(Row row) {
        return row.toBytes();
    }

    /**
     * 反序列化叶子节点的Row数据(辅助方法)
     *
     * @param rowBytes Row字节数组
     * @param columns 列定义
     * @return Row对象
     */
    public static Row deserializeRow(
            byte[] rowBytes, java.util.List<Column> columns) {
        return Row.fromBytes(columns, rowBytes);
    }
}
