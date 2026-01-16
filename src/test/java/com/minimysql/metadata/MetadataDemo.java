package com.minimysql.metadata;

import com.minimysql.storage.StorageEngine;
import com.minimysql.storage.impl.InnoDBStorageEngine;
import com.minimysql.storage.table.Column;
import com.minimysql.storage.table.DataType;
import com.minimysql.storage.table.Row;
import com.minimysql.storage.table.Table;

import java.util.Arrays;
import java.util.List;

/**
 * 元数据管理演示
 *
 * 展示如何使用SchemaManager进行元数据持久化。
 */
public class MetadataDemo {

    public static void main(String[] args) {
        System.out.println("=== Mini MySQL 元数据管理演示 ===\n");

        // 1. 创建存储引擎(启用元数据持久化)
        System.out.println("1. 创建存储引擎...");
        StorageEngine storageEngine = new InnoDBStorageEngine(100, true);
        System.out.println("   ✓ 引擎创建成功\n");

        // 2. 创建业务表
        System.out.println("2. 创建业务表 'users'...");
        List<Column> userColumns = Arrays.asList(
                new Column("id", DataType.INT, false),
                new Column("username", DataType.VARCHAR, 50, false),
                new Column("email", DataType.VARCHAR, 100, true),
                new Column("age", DataType.INT, false)
        );

        Table usersTable = storageEngine.createTable("users", userColumns);
        System.out.println("   ✓ 表创建成功");
        System.out.println("   - 表名: " + usersTable.getTableName());
        System.out.println("   - 表ID: " + usersTable.getTableId());
        System.out.println("   - 列数: " + usersTable.getColumnCount());
        System.out.println();

        // 3. 插入数据
        System.out.println("3. 插入数据...");
        Object[] user1 = {1, "alice", "alice@example.com", 25};
        Object[] user2 = {2, "bob", "bob@example.com", 30};
        Object[] user3 = {3, "charlie", null, 35};

        usersTable.insertRow(new Row(userColumns, user1));
        usersTable.insertRow(new Row(userColumns, user2));
        usersTable.insertRow(new Row(userColumns, user3));
        System.out.println("   ✓ 插入3行数据\n");

        // 4. 查询数据
        System.out.println("4. 查询数据...");
        Row result = usersTable.selectByPrimaryKey(1);
        if (result != null) {
            System.out.println("   查询主键=1的记录:");
            System.out.println("   - id: " + result.getValue(0));
            System.out.println("   - username: " + result.getValue(1));
            System.out.println("   - email: " + result.getValue(2));
            System.out.println("   - age: " + result.getValue(3));
        }
        System.out.println();

        // 5. 创建另一个表
        System.out.println("5. 创建业务表 'posts'...");
        List<Column> postColumns = Arrays.asList(
                new Column("id", DataType.INT, false),
                new Column("title", DataType.VARCHAR, 200, false),
                new Column("content", DataType.VARCHAR, 1000, true),
                new Column("user_id", DataType.INT, false)
        );

        Table postsTable = storageEngine.createTable("posts", postColumns);
        System.out.println("   ✓ 表创建成功");
        System.out.println("   - 表名: " + postsTable.getTableName());
        System.out.println("   - 表ID: " + postsTable.getTableId());
        System.out.println();

        // 6. 列出所有表
        System.out.println("6. 列出所有表...");
        List<String> tableNames = storageEngine.getAllTableNames();
        System.out.println("   共有 " + tableNames.size() + " 个表:");
        for (String tableName : tableNames) {
            Table table = storageEngine.getTable(tableName);
            System.out.println("   - " + tableName + " (ID=" + table.getTableId() + ", 列数=" + table.getColumnCount() + ")");
        }
        System.out.println();

        // 7. 验证系统表存在
        System.out.println("7. 验证系统表存在...");
        System.out.println("   - SYS_TABLES: " + (storageEngine.tableExists(SystemTables.SYS_TABLES) ? "✓" : "✗"));
        System.out.println("   - SYS_COLUMNS: " + (storageEngine.tableExists(SystemTables.SYS_COLUMNS) ? "✓" : "✗"));
        System.out.println();

        // 8. 尝试删除系统表(应该失败)
        System.out.println("8. 尝试删除系统表(应该失败)...");
        try {
            storageEngine.dropTable(SystemTables.SYS_TABLES);
            System.out.println("   ✗ 意外成功(不应该发生)");
        } catch (IllegalArgumentException e) {
            System.out.println("   ✓ 正确拒绝删除系统表");
        }
        System.out.println();

        // 9. 尝试创建系统表名的表(应该失败)
        System.out.println("9. 尝试创建系统表名的表(应该失败)...");
        try {
            List<Column> dummyColumns = Arrays.asList(new Column("id", DataType.INT, false));
            storageEngine.createTable("SYS_TABLES", dummyColumns);
            System.out.println("   ✗ 意外成功(不应该发生)");
        } catch (IllegalArgumentException e) {
            System.out.println("   ✓ 正确拒绝创建系统表名");
        }
        System.out.println();

        // 10. 关闭引擎
        System.out.println("10. 关闭引擎...");
        storageEngine.close();
        System.out.println("   ✓ 引擎已关闭\n");

        System.out.println("=== 演示完成 ===");
        System.out.println("\n注意:");
        System.out.println("- 系统表(SYS_TABLES, SYS_COLUMNS)已自动创建");
        System.out.println("- 业务表的元数据已持久化到系统表");
        System.out.println("- 系统表受到保护，不能被删除或重名");
        System.out.println("- 重启引擎后，元数据可以被恢复(需要实现loadAllMetadata())");
    }
}
