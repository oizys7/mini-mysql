package com.minimysql.storage.page;

import java.nio.ByteBuffer;

/**
 * Page接口 - 数据库页面的抽象
 *
 * 所有页面固定大小为16KB (16384字节),这是InnoDB的标准页大小。
 * 页是数据库存储的最小单位,所有读写操作都以页为单位。
 *
 * 设计哲学:
 * - 页是固定大小的内存块,简化内存管理
 * - 页是独立的,可以单独换入换出缓冲池
 * - 页序列化后直接写入文件,零拷贝设计
 */
public interface Page {

    /** 标准页大小:16KB */
    int PAGE_SIZE = 16384;

    /** 最大页号:Integer.MAX_VALUE */
    int MAX_PAGE_ID = Integer.MAX_VALUE;

    /**
     * 获取页号
     *
     * 页号是页的唯一标识,在文件中作为偏移量计算依据。
     * 例如:页号5的数据从文件偏移量 5 * 16384 处开始。
     */
    int getPageId();

    /**
     * 设置页号
     *
     * 仅在从文件读取页时调用,用户不应手动修改。
     */
    void setPageId(int pageId);

    /**
     * 获取页的原始字节数据
     *
     * 返回的是内部字节数组的引用,用于直接写入文件。
     * 外部调用者不应修改此数组的内容。
     */
    byte[] getData();

    /**
     * 从字节数组反序列化页
     *
     * 从文件读取页后调用此方法恢复页的内部结构。
     * 必须确保data.length == PAGE_SIZE。
     */
    void fromBytes(byte[] data);

    /**
     * 将页序列化为字节数组
     *
     * 返回的字节数组可以直接写入文件。
     * 等价于getData(),但提供了更明确的语义。
     */
    byte[] toBytes();

    /**
     * 获取页的类型
     *
     * 页类型用于区分不同用途的页:
     * - 数据页(DataPage):存储表行数据
     * - 索引页(IndexPage):存储B+树节点
     * - 其他页面类型可在此扩展
     */
    PageType getType();

    /**
     * 页类型枚举
     *
     * 每个页的前1个字节存储类型标识,用于从文件读取时判断如何解析。
     */
    enum PageType {
        /** 数据页:存储表行数据 */
        DATA_PAGE(0x01),

        /** 索引页:存储B+树内部节点 */
        INDEX_PAGE(0x02),

        /** 未初始化的页 */
        UNINITIALIZED(0x00);

        private final byte code;

        PageType(int code) {
            this.code = (byte) code;
        }

        public byte getCode() {
            return code;
        }

        public static PageType fromCode(byte code) {
            for (PageType type : values()) {
                if (type.code == code) {
                    return type;
                }
            }
            throw new IllegalArgumentException("Unknown page type code: " + code);
        }
    }
}
