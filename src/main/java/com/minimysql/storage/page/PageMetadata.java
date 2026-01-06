package com.minimysql.storage.page;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;

/**
 * PageMetadata - 页管理器元数据
 *
 * PageManager的元数据,用于持久化和恢复页分配状态。
 *
 * 文件格式:
 * +------------------+ <- 0
 * | Magic (4 bytes)  |  魔数: 0x50474554 ("PGMT")
 * | Version (1 byte) |  版本: 0x01
 * | Reserved (3 bytes)|  保留字段(对齐)
 * +------------------+ <- 8
 * | nextPageId (4)   |  下一个新页号
 * | freePagesCount (4)|  空闲页数量
 * +------------------+ <- 16
 * | freePageIds...   |  空闲页ID列表(每个4字节)
 * | (N * 4 bytes)    |
 * +------------------+
 *
 * 设计哲学:
 * - 简单的二进制格式,无需复杂的序列化库
 * - 固定头部 + 变长空闲页列表
 * - 魔数用于验证文件类型
 * - 版本号用于未来扩展
 *
 * "Good taste": 没有复杂的压缩、加密,只有最基本的数据存储
 */
public class PageMetadata {

    /** 魔数: "PGMT" (PageManager MeTadata) */
    public static final int MAGIC = 0x50474d54;

    /** 当前版本: 0x01 */
    public static final int VERSION = 0x01;

    /** 头部大小: 魔数(4) + 版本(1) + 保留(3) + nextPageId(4) + freePagesCount(4) = 16字节 */
    public static final int HEADER_SIZE = 16;

    /** 下一个新页号 */
    private final int nextPageId;

    /** 空闲页集合 */
    private final Set<Integer> freePages;

    /**
     * 创建元数据
     *
     * @param nextPageId 下一个新页号
     * @param freePages 空闲页集合
     */
    public PageMetadata(int nextPageId, Set<Integer> freePages) {
        this.nextPageId = nextPageId;
        this.freePages = new HashSet<>(freePages); // defensive copy
    }

    /**
     * 从字节数组反序列化元数据
     *
     * @param data 字节数组
     * @return 元数据对象
     * @throws IllegalArgumentException 如果数据格式无效
     */
    public static PageMetadata fromBytes(byte[] data) {
        if (data.length < HEADER_SIZE) {
            throw new IllegalArgumentException("Invalid metadata: data too short, size=" + data.length);
        }

        ByteBuffer buffer = ByteBuffer.wrap(data);

        // 读取魔数
        int magic = buffer.getInt();
        if (magic != MAGIC) {
            throw new IllegalArgumentException("Invalid metadata: wrong magic number, found=0x" +
                    Integer.toHexString(magic) + ", expected=0x" + Integer.toHexString(MAGIC));
        }

        // 读取版本号
        int version = buffer.get() & 0xFF; // unsigned byte
        if (version != VERSION) {
            throw new IllegalArgumentException("Invalid metadata: unsupported version=" + version +
                    ", expected=" + VERSION);
        }

        // 跳过保留字段(3字节)
        buffer.position(buffer.position() + 3);

        // 读取nextPageId
        int nextPageId = buffer.getInt();

        // 读取空闲页数量
        int freePagesCount = buffer.getInt();

        // 验证数据长度
        int expectedLength = HEADER_SIZE + freePagesCount * 4;
        if (data.length != expectedLength) {
            throw new IllegalArgumentException("Invalid metadata: length mismatch, expected=" +
                    expectedLength + ", actual=" + data.length);
        }

        // 读取空闲页列表
        Set<Integer> freePages = new HashSet<>();
        for (int i = 0; i < freePagesCount; i++) {
            int pageId = buffer.getInt();
            freePages.add(pageId);
        }

        return new PageMetadata(nextPageId, freePages);
    }

    /**
     * 序列化为字节数组
     *
     * @return 字节数组
     */
    public byte[] toBytes() {
        ByteBuffer buffer = ByteBuffer.allocate(HEADER_SIZE + freePages.size() * 4);

        // 写入魔数
        buffer.putInt(MAGIC);

        // 写入版本号
        buffer.put((byte) VERSION);

        // 写入保留字段(3字节)
        buffer.putShort((short) 0);
        buffer.put((byte) 0);

        // 写入nextPageId
        buffer.putInt(nextPageId);

        // 写入空闲页数量
        buffer.putInt(freePages.size());

        // 写入空闲页列表
        for (int pageId : freePages) {
            buffer.putInt(pageId);
        }

        return buffer.array();
    }

    /**
     * 获取下一个新页号
     */
    public int getNextPageId() {
        return nextPageId;
    }

    /**
     * 获取空闲页集合
     */
    public Set<Integer> getFreePages() {
        return new HashSet<>(freePages); // defensive copy
    }

    @Override
    public String toString() {
        return "PageMetadata{" +
                "nextPageId=" + nextPageId +
                ", freePages=" + freePages.size() +
                '}';
    }
}
