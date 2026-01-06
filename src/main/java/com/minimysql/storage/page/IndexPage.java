package com.minimysql.storage.page;

import com.minimysql.storage.index.BPlusTreeNode;

import java.nio.ByteBuffer;

/**
 * IndexPage - 索引页实现
 *
 * 索引页用于存储B+树节点。每个页固定16KB,存储一个完整的B+树节点。
 *
 * 与DataPage的区别:
 * - DataPage: 存储表行数据(变长、多个Row、槽位结构)
 * - IndexPage: 存储B+树节点(固定结构、单个节点、序列化数据)
 *
 * 页布局:
 * +------------------+ <- 0
 * | PageType (1 byte)|  0x02 (INDEX_PAGE)
 * +------------------+ <- 1
 * | PageId (4 bytes) |
 * +------------------+ <- 5
 * | Reserved (7)      |
 * +------------------+ <- 12
 * | NodeData         |  BPlusTreeNode序列化数据
 * | (16372 bytes)   |
 * +------------------+ <- 16384
 *
 * 设计哲学:
 * - 索引页只存储一个B+树节点(内部或叶子)
 * - 节点序列化由BPlusTreeNode.toBytes()完成
 * - 页头仅存储类型和页号,简化结构
 * - 无需槽位表和空间管理(节点大小固定)
 *
 * "Good taste": 索引页是B+树节点的容器,职责单一,无特殊情况
 */
public class IndexPage implements Page {

    /** 页头大小:类型(1) + 页号(4) + 保留(7) = 12字节 */
    public static final int HEADER_SIZE = 12;

    /** 页的原始数据 */
    private final byte[] data;

    /** 页号 */
    private int pageId;

    /** B+树节点(内存中缓存) */
    private BPlusTreeNode node;

    /**
     * 创建一个新的空索引页
     */
    public IndexPage() {
        this.data = new byte[PAGE_SIZE];
        this.pageId = -1;
        this.node = null;

        // 写入页类型
        data[0] = PageType.INDEX_PAGE.getCode();

        // 初始化页头
        serializeHeader();
    }

    /**
     * 从B+树节点创建索引页
     *
     * @param node B+树节点
     */
    public IndexPage(BPlusTreeNode node) {
        this();
        this.node = node;
        serializeNode();
    }

    /**
     * 从字节数组恢复索引页
     *
     * 从文件读取页后调用此方法。
     *
     * 用法示例:
     * IndexPage page = new IndexPage();
     * page.fromBytes(dataFromFile);
     */
    @Override
    public int getPageId() {
        return pageId;
    }

    @Override
    public void setPageId(int pageId) {
        this.pageId = pageId;
        serializeHeader();
    }

    @Override
    public byte[] getData() {
        return data;
    }

    @Override
    public void fromBytes(byte[] data) {
        if (data.length != PAGE_SIZE) {
            throw new IllegalArgumentException(
                    "Invalid page size: expected " + PAGE_SIZE + ", got " + data.length);
        }

        // 复制数据
        System.arraycopy(data, 0, this.data, 0, PAGE_SIZE);

        // 读取页头
        deserializeHeader();
    }

    @Override
    public byte[] toBytes() {
        return data;
    }

    @Override
    public PageType getType() {
        return PageType.INDEX_PAGE;
    }

    /**
     * 获取B+树节点
     *
     * 如果节点尚未反序列化,从data中加载。
     *
     * @return B+树节点
     */
    public BPlusTreeNode getNode() {
        if (node == null) {
            // 从字节数组反序列化节点
            byte[] nodeData = new byte[PAGE_SIZE - HEADER_SIZE];
            System.arraycopy(data, HEADER_SIZE, nodeData, 0, nodeData.length);
            node = BPlusTreeNode.fromBytes(nodeData);
            node.setPageId(pageId);
        }
        return node;
    }

    /**
     * 设置B+树节点
     *
     * 序列化节点并写入页数据。
     *
     * @param node B+树节点
     */
    public void setNode(BPlusTreeNode node) {
        this.node = node;
        serializeNode();
    }

    /**
     * 序列化页头
     *
     * 写入页号到页头。
     */
    private void serializeHeader() {
        ByteBuffer buffer = ByteBuffer.wrap(data);
        buffer.position(1); // 跳过PageType

        // 写入页号(4 bytes)
        buffer.putInt(pageId);

        // 保留字段(7 bytes) - 暂时填充0
        buffer.position(HEADER_SIZE);
    }

    /**
     * 反序列化页头
     *
     * 从data中读取页号。
     */
    private void deserializeHeader() {
        ByteBuffer buffer = ByteBuffer.wrap(data);
        buffer.position(1); // 跳过PageType

        // 读取页号(4 bytes)
        this.pageId = buffer.getInt();

        // 跳过保留字段(7 bytes)
        buffer.position(HEADER_SIZE);
    }

    /**
     * 序列化B+树节点到页数据
     *
     * 将节点序列化后写入页的NodeData区域。
     */
    private void serializeNode() {
        if (node == null) {
            return;
        }

        // 序列化节点
        byte[] nodeData = node.toBytes();

        // 写入页的NodeData区域(跳过页头)
        System.arraycopy(nodeData, 0, data, HEADER_SIZE, nodeData.length);

        // 清零剩余空间(避免旧数据干扰)
        if (nodeData.length < PAGE_SIZE - HEADER_SIZE) {
            java.util.Arrays.fill(data, HEADER_SIZE + nodeData.length, PAGE_SIZE, (byte) 0);
        }
    }

    /**
     * 清空页(重置为初始状态)
     *
     * 保留页号,清空节点数据。
     */
    public void clear() {
        this.node = null;

        // 清零NodeData区域(保留页头)
        java.util.Arrays.fill(data, HEADER_SIZE, PAGE_SIZE, (byte) 0);
    }

    @Override
    public String toString() {
        return "IndexPage{" +
                "pageId=" + pageId +
                ", hasNode=" + (node != null) +
                ", type=" + getType() +
                '}';
    }
}
