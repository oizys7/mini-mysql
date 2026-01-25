package com.minimysql.storage.page;

import java.util.ArrayList;
import java.util.List;

/**
 * DataPage - 数据页 (Data Page)
 *
 * <p>数据页用于存储表中的行数据记录 (Physical Records)。每个页固定 16KB，使用经典的"槽位 + 数据"布局。
 *
 * <p>MySQL InnoDB 对应关系:
 * <ul>
 *   <li>DataPage → InnoDB Data Page (索引页叶子节点)</li>
 *   <li>16KB 页大小 → InnoDB 默认页大小 (innodb_page_size)</li>
 *   <li>Slot → Page Directory Slot (页目录槽位)</li>
 *   <li>Row Data → Physical Record (物理记录)</li>
 * </ul>
 *
 * <p>InnoDB 页结构 (简化版):
 * <pre>
 * +------------------+ <- 0      (FIL Header - 38 bytes)
 * | Page Type (1)    |  页类型
 * | Page ID (4)      |  页号
 * | ...              |  其他元数据
 * +------------------+ <- 38     (Page Header - 56 bytes)
 * | Page Header      |  页头信息
 * | - N_DIR_SLOTS (2)|  目录槽数
 * | - HEAP_TOP (2)   |  堆顶指针
 * | - ...            |  其他页头信息
 * +------------------+ <- 94     (Infimum + Supremum - 26 bytes)
 * | Infimum Record   |  最小记录
 * | Supremum Record  |  最大记录
 * +------------------+ <- 120    (Records - 变长)
 * | User Records     |  用户记录区域
 * | - Record 0       |  物理记录 (从后往前生长)
 * | - Record 1       |
 * | - ...            |
 * +------------------+ <- 变长   (Free Space - 变长)
 * | Free Space       |  空闲空间
 * +------------------+ <- ~16300 (Page Directory - 变长)
 * | Page Directory   |  页目录 (槽位数组)
 * | - Slot[0] (2)    |  指向 Record 0 的偏移量
 * | - Slot[1] (2)    |  指向 Record 1 的偏移量
 * | - ...            |
 * +------------------+ <- 16376  (FIL Trailer - 8 bytes)
 * | FIL Trailer      |  文件尾 (CHECKSUM 等)
 * +------------------+ <- 16384 (16KB)
 * </pre>
 *
 * <p>简化实现 (Mini MySQL):
 * <ul>
 *   <li>不实现完整的 FIL Header/Page Header</li>
 *   <li>不实现 Infimum/Supremum 记录</li>
 *   <li>简化页目录结构 (只保留槽位数组)</li>
 *   <li>记录格式: [4字节长度] + [实际数据]</li>
 * </ul>
 *
 * <p>参考文档:
 * <ul>
 *   <li>https://dev.mysql.com/doc/refman/8.0/en/innodb-page-structure.html</li>
 *   <li>https://dev.mysql.com/doc/refman/8.0/en/innodb-data-structures.html</li>
 * </ul>
 *
 * <p>设计哲学:
 * <ul>
 *   <li>行数据从页尾向前生长，槽位表从页头向后生长，中间是自由空间</li>
 *   <li>删除行时只需要将对应槽位设为 0，不需要移动数据 (碎片化由后续整理解决)</li>
 *   <li>行数据可变长，每个行头存储长度信息</li>
 * </ul>
 *
 * <p>"Good taste": 没有特殊情况，所有行都通过槽位访问，删除、插入逻辑统一
 */
public class DataPage implements Page {

    /** 页头大小:类型(1) + 页号(4) + 自由空间结束位置(4) + 槽位数(2) = 11字节 */
    private static final int HEADER_SIZE = 11;

    /** 槽位大小:2字节(短整型) */
    private static final int SLOT_SIZE = 2;

    /** 行头大小:4字节(存储行数据长度) */
    private static final int ROW_HEADER_SIZE = 4;

    /** 页的原始数据 */
    private final byte[] data;

    /** 页号 */
    private int pageId;

    /** 自由空间结束位置(从页尾开始计算) */
    private int freeSpaceEnd;

    /** 已使用的槽位数 */
    private int slotCount;

    /**
     * 创建一个新的空数据页
     */
    public DataPage() {
        this.data = new byte[PAGE_SIZE];
        this.pageId = -1;
        this.freeSpaceEnd = PAGE_SIZE;
        this.slotCount = 0;

        // 写入页类型
        data[0] = PageType.DATA_PAGE.getCode();

        // 初始化页头
        serializeHeader();
    }

    /**
     * 从字节数组恢复数据页
     *
     * 从文件读取页后调用此方法,恢复页的内部结构(页号、自由空间位置、槽位表等)
     *
     * 用法示例:
     * DataPage page = new DataPage();
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
        System.arraycopy(data, 0, this.data, 0, PAGE_SIZE);
        deserializeHeader();
    }

    @Override
    public byte[] toBytes() {
        serializeHeader();
        return data;
    }

    @Override
    public PageType getType() {
        return PageType.DATA_PAGE;
    }

    /**
     * 插入一行数据
     *
     * 将行数据插入到页中,返回分配的槽位号。
     * 如果空间不足,抛出异常(不处理跨页存储,让上层实现)。
     *
     * @param row 行数据(字节数组)
     * @return 分配的槽位号
     * @throws IllegalStateException 如果页没有足够空间
     */
    public int insertRow(byte[] row) {
        int requiredSpace = ROW_HEADER_SIZE + row.length;

        if (!hasFreeSpace(requiredSpace)) {
            throw new IllegalStateException("Page is full: cannot insert row of size " + row.length);
        }

        // 计算新行的插入位置(从页尾向前生长)
        int newRowOffset = freeSpaceEnd - requiredSpace;

        // 写入行头(行长度)
        writeInt(newRowOffset, row.length);

        // 写入行数据
        System.arraycopy(row, 0, data, newRowOffset + ROW_HEADER_SIZE, row.length);

        // 更新自由空间结束位置
        freeSpaceEnd = newRowOffset;

        // 添加新槽位
        int slotNumber = slotCount;
        slotCount++;
        setSlotOffset(slotNumber, newRowOffset);

        // 序列化页头
        serializeHeader();

        return slotNumber;
    }

    /**
     * 读取一行数据
     *
     * 通过槽位号读取行数据。如果槽位无效或已删除,返回null。
     *
     * @param slotNumber 槽位号
     * @return 行数据,如果槽位无效返回null
     */
    public byte[] getRow(int slotNumber) {
        if (slotNumber < 0 || slotNumber >= slotCount) {
            return null;
        }

        int rowOffset = getSlotOffset(slotNumber);

        // 槽位为0表示已删除
        if (rowOffset == 0) {
            return null;
        }

        // 读取行头(行长度)
        int rowLength = readInt(rowOffset);

        // 读取行数据
        byte[] row = new byte[rowLength];
        System.arraycopy(data, rowOffset + ROW_HEADER_SIZE, row, 0, rowLength);

        return row;
    }

    /**
     * 删除一行数据
     *
     * 将槽位设为0标记为删除,不释放空间(碎片化由后续整理解决)。
     *
     * @param slotNumber 槽位号
     * @return true如果删除成功,false如果槽位无效
     */
    public boolean deleteRow(int slotNumber) {
        if (slotNumber < 0 || slotNumber >= slotCount) {
            return false;
        }

        setSlotOffset(slotNumber, 0);
        return true;
    }

    /**
     * 获取页中所有有效行
     *
     * 返回所有未被删除的行数据。
     */
    public List<byte[]> getAllRows() {
        List<byte[]> rows = new ArrayList<>();

        for (int i = 0; i < slotCount; i++) {
            byte[] row = getRow(i);
            if (row != null) {
                rows.add(row);
            }
        }

        return rows;
    }

    /**
     * 获取行数(包括已删除的)
     */
    public int getRowCount() {
        return slotCount;
    }

    /**
     * 获取有效行数(不包括已删除的)
     */
    public int getValidRowCount() {
        int count = 0;
        for (int i = 0; i < slotCount; i++) {
            if (getSlotOffset(i) != 0) {
                count++;
            }
        }
        return count;
    }

    /**
     * 获取自由空间大小
     */
    public int getFreeSpace() {
        int slotTableEnd = HEADER_SIZE + slotCount * SLOT_SIZE;
        return freeSpaceEnd - slotTableEnd;
    }

    /**
     * 检查是否有足够的自由空间
     */
    public boolean hasFreeSpace(int requiredBytes) {
        return getFreeSpace() >= requiredBytes + SLOT_SIZE;
    }

    /**
     * 序列化页头到字节数组
     *
     * 页头布局:
     * +-------------------+
     * | PageType (1 byte) |
     * | PageId (4 bytes)  |
     * | FreeSpaceEnd (4)  |
     * | SlotCount (2)     |
     * +-------------------+
     */
    private void serializeHeader() {
        data[0] = PageType.DATA_PAGE.getCode();
        writeInt(1, pageId);
        writeInt(5, freeSpaceEnd);
        writeShort(9, slotCount);
    }

    /**
     * 从字节数组反序列化页头
     */
    private void deserializeHeader() {
        // 验证页类型
        PageType type = PageType.fromCode(data[0]);
        if (type != PageType.DATA_PAGE) {
            throw new IllegalArgumentException("Invalid page type: expected DATA_PAGE, got " + type);
        }

        pageId = readInt(1);
        freeSpaceEnd = readInt(5);
        slotCount = readShort(9);
    }

    /**
     * 获取槽位指向的行偏移量
     */
    private int getSlotOffset(int slotNumber) {
        int slotOffset = HEADER_SIZE + slotNumber * SLOT_SIZE;
        return readShort(slotOffset);
    }

    /**
     * 设置槽位指向的行偏移量
     */
    private void setSlotOffset(int slotNumber, int rowOffset) {
        int slotOffset = HEADER_SIZE + slotNumber * SLOT_SIZE;
        writeShort(slotOffset, rowOffset);
    }

    /**
     * 在指定偏移量读取一个整数(4字节,小端序)
     */
    private int readInt(int offset) {
        return ((data[offset] & 0xFF)) |
               ((data[offset + 1] & 0xFF) << 8) |
               ((data[offset + 2] & 0xFF) << 16) |
               ((data[offset + 3] & 0xFF) << 24);
    }

    /**
     * 在指定偏移量写入一个整数(4字节,小端序)
     */
    private void writeInt(int offset, int value) {
        data[offset] = (byte) (value & 0xFF);
        data[offset + 1] = (byte) ((value >> 8) & 0xFF);
        data[offset + 2] = (byte) ((value >> 16) & 0xFF);
        data[offset + 3] = (byte) ((value >> 24) & 0xFF);
    }

    /**
     * 在指定偏移量读取一个短整数(2字节,小端序)
     */
    private int readShort(int offset) {
        return ((data[offset] & 0xFF)) |
               ((data[offset + 1] & 0xFF) << 8);
    }

    /**
     * 在指定偏移量写入一个短整数(2字节,小端序)
     */
    private void writeShort(int offset, int value) {
        data[offset] = (byte) (value & 0xFF);
        data[offset + 1] = (byte) ((value >> 8) & 0xFF);
    }
}
