package com.minimysql.storage.table;

import com.minimysql.storage.buffer.BufferPool;
import com.minimysql.storage.page.PageManager;

import java.util.Arrays;
import java.util.Date;
import java.util.List;

/**
 * MultiTableExample - 多表共享BufferPool示例
 *
 * 演示如何创建多个表共享同一个全局BufferPool,
 * 这与MySQL InnoDB的设计一致。
 *
 * MySQL InnoDB架构对应:
 * - BufferPool → InnoDB Buffer Pool (全局共享,所有表共用)
 * - Table → InnoDB Table
 * - PageManager → InnoDB Table Space (每表独立)
 *
 * 关键点:
 * 1. BufferPool只创建一次,全局共享
 * 2. 每个表有独立的PageManager
 * 3. 所有表的页通过(tableId, pageId)唯一标识
 * 4. LRU算法跨所有表工作
 */
public class MultiTableExample {

    public static void main(String[] args) {
        // 1. 创建全局共享的BufferPool (所有表共用)
        //    对应MySQL的: innodb_buffer_pool_size
        BufferPool globalBufferPool = new BufferPool(100); // 100页 = 1.6MB

        System.out.println("=== 创建全局BufferPool (所有表共享) ===");
        System.out.println("BufferPool大小: " + globalBufferPool.getPoolSize() + "页");
        System.out.println();

        // 2. 创建表1: users
        List<Column> userColumns = Arrays.asList(
                new Column("id", DataType.INT, false),
                new Column("username", DataType.VARCHAR, 50, false),
                new Column("email", DataType.VARCHAR, 100, true),
                new Column("age", DataType.INT, true),
                new Column("created_at", DataType.TIMESTAMP, true)
        );

        PageManager userPageManager = new PageManager();
        userPageManager.load(0); // tableId = 0

        Table usersTable = new Table(0, "users", userColumns);
        usersTable.open(globalBufferPool, userPageManager);

        System.out.println("=== 创建表: users ===");
        System.out.println("表ID: " + usersTable.getTableId());
        System.out.println("列数: " + usersTable.getColumnCount());
        System.out.println();

        // 3. 创建表2: orders
        List<Column> orderColumns = Arrays.asList(
                new Column("id", DataType.INT, false),
                new Column("user_id", DataType.INT, false),
                new Column("product_name", DataType.VARCHAR, 200, false),
                new Column("amount", DataType.DOUBLE, false),
                new Column("order_date", DataType.DATE, true)
        );

        PageManager orderPageManager = new PageManager();
        orderPageManager.load(1); // tableId = 1

        Table ordersTable = new Table(1, "orders", orderColumns);
        ordersTable.open(globalBufferPool, orderPageManager);

        System.out.println("=== 创建表: orders ===");
        System.out.println("表ID: " + ordersTable.getTableId());
        System.out.println("列数: " + ordersTable.getColumnCount());
        System.out.println();

        // 4. 向users表插入数据
        System.out.println("=== 向users表插入数据 ===");
        for (int i = 0; i < 5; i++) {
            Object[] values = {
                    i + 1,
                    "user" + i,
                    "user" + i + "@example.com",
                    20 + i,
                    new Date()
            };

            Row row = new Row(userColumns, values);
            int pageId = usersTable.insertRow(row);

            System.out.println("插入用户 " + values[1] + " → 页号: " + pageId);
        }
        System.out.println();

        // 5. 向orders表插入数据
        System.out.println("=== 向orders表插入数据 ===");
        for (int i = 0; i < 8; i++) {
            Object[] values = {
                    i + 1,
                    (i % 5) + 1, // 关联到users表
                    "Product " + i,
                    100.0 + i * 10,
                    new Date()
            };

            Row row = new Row(orderColumns, values);
            int pageId = ordersTable.insertRow(row);

            System.out.println("插入订单 " + values[2] + " → 页号: " + pageId);
        }
        System.out.println();

        // 6. 查看BufferPool状态
        System.out.println("=== BufferPool状态 ===");
        System.out.println("缓存的页数: " + globalBufferPool.getCacheSize());
        System.out.println("BufferPool大小: " + globalBufferPool.getPoolSize() + "页");
        System.out.println();

        // 7. 查看各表的页分配情况
        System.out.println("=== 表空间页分配 ===");
        System.out.println("users表: " + userPageManager.getAllocatedPageCount() + "页已分配");
        System.out.println("orders表: " + orderPageManager.getAllocatedPageCount() + "页已分配");
        System.out.println();

        // 8. 关闭表
        System.out.println("=== 关闭表 ===");
        usersTable.close();
        ordersTable.close();

        // 注意:BufferPool不在这里关闭,它是全局共享的
        // 应该在应用关闭时才关闭

        System.out.println("完成!");
        System.out.println();
        System.out.println("关键点:");
        System.out.println("✓ BufferPool全局共享,所有表共用");
        System.out.println("✓ 每个表有独立的PageManager");
        System.out.println("✓ 页用(tableId, pageId)唯一标识");
        System.out.println("✓ LRU算法跨所有表工作");
        System.out.println("✓ 与MySQL InnoDB架构一致");
    }
}
