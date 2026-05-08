package com.minimysql.storage.table;

import com.minimysql.storage.buffer.BufferPool;
import com.minimysql.storage.index.ClusteredIndex;
import com.minimysql.storage.page.PageManager;

import java.util.Arrays;
import java.util.List;

/**
 * TableFixture - 表测试辅助类
 *
 * 封装测试中常用的表创建逻辑，避免重复代码。
 */
public class TableFixture {

    private final int tableId;
    private final BufferPool bufferPool;
    private final PageManager pageManager;
    private Table table;

    /**
     * 创建默认的测试Fixture
     */
    public TableFixture() {
        this(100);
    }

    /**
     * 创建指定表ID的测试Fixture
     *
     * @param tableId 表ID
     */
    public TableFixture(int tableId) {
        this.tableId = tableId;
        this.bufferPool = new BufferPool(10);
        this.pageManager = new PageManager();
        this.pageManager.load(tableId);
    }

    /**
     * 创建包含多列的默认测试表
     *
     * @return Table实例
     */
    public Table createDefaultTable() {
        List<Column> columns = Arrays.asList(
                new Column("id", DataType.INT, false),
                new Column("name", DataType.VARCHAR, 100, true),
                new Column("age", DataType.INT, false),
                new Column("balance", DataType.DOUBLE, true),
                new Column("active", DataType.BOOLEAN, false),
                new Column("created_at", DataType.TIMESTAMP, true)
        );
        return createTable("users", columns);
    }

    /**
     * 创建自定义表
     *
     * @param tableName 表名
     * @param columns 列定义
     * @return Table实例
     */
    public Table createTable(String tableName, List<Column> columns) {
        this.table = new Table(tableId, tableName, columns);
        table.open(bufferPool, pageManager);

        // 创建聚簇索引
        ClusteredIndex clusteredIndex = new ClusteredIndex(
                tableId,
                columns.get(0).getName(),  // 第一列作为主键
                0,                          // 第一列索引
                bufferPool,
                pageManager
        );
        clusteredIndex.setTable(table);
        table.setClusteredIndex(clusteredIndex);

        return table;
    }

    /**
     * 获取BufferPool
     */
    public BufferPool getBufferPool() {
        return bufferPool;
    }

    /**
     * 获取PageManager
     */
    public PageManager getPageManager() {
        return pageManager;
    }

    /**
     * 获取创建的Table
     */
    public Table getTable() {
        return table;
    }

    /**
     * 清理资源
     */
    public void cleanup() {
        if (table != null) {
            table.close();
        }
        pageManager.deleteMetadata();
    }
}
